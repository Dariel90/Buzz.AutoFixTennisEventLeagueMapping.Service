using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Buzz.AutoFixTennisEventLeagueMapping.WService.Dgs;
using Buzz.TxLeague.Women.Config.Lineshouse;
using Microsoft.EntityFrameworkCore;
using Renci.SshNet;
using SlackAPI;

namespace Buzz.AutoFixTennisEventLeagueMapping.WService
{
    internal class TennisEventsHandler : ITennisAutoFixEventLeagueMap
    {
        private readonly LineshouseContext lineshouseContext;
        private readonly DgsContext dgsContext;
        private readonly SlackTaskClient slackClient;

        public TennisEventsHandler(LineshouseContext lineshouseContext, DgsContext dgsContext)
        {
            this.lineshouseContext = lineshouseContext;
            this.dgsContext = dgsContext;
            this.slackClient = new SlackTaskClient("xoxb-1244510899716-4418863659349-kFTsHvlz23Q6uPhxGUQYwhBH");
        }

        public async Task Handle(CancellationToken cancellationToken)
        {
            //this._dgsContext.Config.Load();
            //this._dgsContext.League.Load();
            using (var transaction = this.lineshouseContext.Database.BeginTransaction())
            {
                var query = $"SELECT `lineshouse-prod`.Events.Id as EvId,`lineshouse-prod`.BetradarEventMaps.TournamentId" +
                            " FROM `lineshouse-prod`.Fixtures " +
                            "JOIN `lineshouse-prod`.Events ON `lineshouse-prod`.Events.Id = `lineshouse-prod`.Fixtures.EventId " +
                            "JOIN `lineshouse-prod`.BetradarEventMaps ON `lineshouse-prod`.Events.Id = `lineshouse-prod`.BetradarEventMaps.ExtEventId where `lineshouse-prod`.Fixtures.IsMain and `lineshouse-prod`.Fixtures.Date > now() and `lineshouse-prod`.Events.LeagueId = 344584 order by `lineshouse-prod`.BetradarEventMaps.TournamentId";
                var tournamentsInDefaultTennisLeague = this.lineshouseContext.TournamentEvents.FromSqlRaw(query).ToListAsync().Result;
                var eventsToProcess = new List<int>();
                var landingDefaultLeaguesEvents = new List<(int eventId, string tournamentId)>();
                if (!tournamentsInDefaultTennisLeague.Any())
                {
                    await this.SendTournamentsForMapToSlack(0, 0, "No tennis leagues landing on the Default League");
                    return;
                }

                foreach (var tournament in tournamentsInDefaultTennisLeague)
                {
                    var league = this.lineshouseContext.BetradarEventMaps.Include(x => x.ExtEvent).ThenInclude(x => x.League).FirstOrDefault(x => x.ExtEvent.League.Id != 344584 && x.TournamentId == tournament.TournamentId)?.ExtEvent.League;
                    if (league == null)
                    {
                        var tournamentId = Convert.ToInt32(tournament.TournamentId.Split(":")[2]);
                        await this.SendTournamentsForMapToSlack(tournamentId, tournament.EvId);
                        landingDefaultLeaguesEvents.Add(new(tournament.EvId, tournament.TournamentId));
                    }
                    else
                    {
                        query = $"UPDATE `lineshouse-prod`.Events SET LeagueId = {league.Id} WHERE Id = {tournament.EvId}";
                        this.lineshouseContext.Database.ExecuteSqlRaw(query);
                        eventsToProcess.Add(tournament.EvId);
                    }
                }
                this.lineshouseContext.SaveChanges();
                transaction.Commit();

                if (eventsToProcess.Any())
                {
                    var eventsToDelete = this.dgsContext.Events.Where(x => eventsToProcess.Contains((int)x.Id)).ToList();
                    this.dgsContext.Events.RemoveRange(eventsToDelete);
                    this.dgsContext.SaveChanges();
                    var events = string.Join(",", eventsToProcess.Select(e => e.ToString()).ToArray());
                    Console.WriteLine(events);
                    await this.RecreateGamesBulkTask(events);
                    await this.SendTournamentsForMapToSlack(0, 0, $"{tournamentsInDefaultTennisLeague.Count} Events landing on the Default Tennis League where Mapped and Pulled to the DGS Database");
                }
            }
        }

        private async Task SendTournamentsForMapToSlack(int tournamentId, int evId, string mymessage = null)
        {
            if (string.IsNullOrEmpty(mymessage))
            {
                string message =
                $@"Missing mapping for Tournament: Id={tournamentId} Event={evId}";

                _ = await this.slackClient.PostMessageAsync("#unmaped-tournament-league", message);
            }
            else
            {
                _ = await this.slackClient.PostMessageAsync("#unmaped-tournament-league", mymessage);
            }
        }

        private Task RecreateGamesBulkTask(string arrayOfEventIds)
        {
            var eventsIdsArray = arrayOfEventIds.Split('\u002C');
            var result = string.Empty;
            var hostname = "10.0.0.181";
            var username = "sysusr";
            var password = "Qev5?AA.";
            using (var client = new SshClient(hostname, username, password))
            {
                client.Connect();
                var iter = 0;
                foreach (var item in eventsIdsArray)
                {
                    var cmd = client.CreateCommand($"cd Test/LineshouseSnapshot; dotnet Ls-dgs-sync.dll {Int32.Parse(item)}");
                    var response = cmd.BeginExecute();

                    using (var reader =
                               new StreamReader(cmd.OutputStream, Encoding.UTF8, true, 1024, true))
                    {
                        while (!response.IsCompleted || !reader.EndOfStream)
                        {
                            string line = reader.ReadLine();
                            if (line != null)
                            {
                                result += line + " | ";
                            }
                        }
                        iter++;
                    }

                    decimal percent = (decimal)((decimal)(iter / (decimal)eventsIdsArray.Length) * 100);
                    Console.WriteLine($"Advance: {Math.Round(percent, 2)}");

                    result.TrimEnd('|');
                }
                client.Disconnect();
            }
            return Task.CompletedTask;
        }
    }
}