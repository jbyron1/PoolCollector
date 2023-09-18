import argparse
from gql import Client, gql, dsl
from gql.transport.requests import RequestsHTTPTransport
import re
import time
import sqlite3

count = 0

def create_db():
    conn = sqlite3.connect('phases.db')
    return conn

def gen_headers():
    try:
        with open('auth.txt', 'r') as auth:
            key = auth.read().strip()
            header = {"Content-Type": "application/json",
                      "Authorization": "Bearer " + key}
            return header
    except FileNotFoundError as e:
        "Could not open auth.txt, please put start.gg api key in auth.txt"


def execute(query, session, vars={}):
    sleepTime = 1
    while True:
        try:
            global count
            count += 1
            return session.execute(query, variable_values=vars)
        except:
            time.sleep(sleepTime)
            sleepTime = 10

def getEvents(session, tournament_slug: str) -> dict:
    """
    Get a dictionary of event ids and event games from a tournament slug
    """
    q = gql("""
    query getEvents($slug: String) {
    tournament(slug: $slug) {
    events {
      id
      videogame {
        name
      }
      name
    }
    }
    }
    """)

    params = {"slug": tournament_slug}

    event_dict = {}

    result = execute(q, session, params)
    for event in result['tournament']['events']:
        event_dict[event['id']] = (event['videogame']['name'], event['name'])

    return event_dict

def getEventPhaseGroups(event_id : int, ds, session) -> dict:
    q = dsl.dsl_gql(
        dsl.DSLQuery(
            ds.Query.event(id=event_id).select(
                ds.Event.tournament.select(
                    ds.Tournament.name,
                    ds.Tournament.slug,
                    ds.Tournament.id
                ),
                ds.Event.phaseGroups.select(
                    ds.PhaseGroup.id,
                    ds.PhaseGroup.phase.select(ds.Phase.id),
                    ds.PhaseGroup.displayIdentifier,
                    #ds.PhaseGroup.wave.select(
                    #    ds.Wave.identifier,
                    #    ds.Wave.startAt
                    #),
                    #ds.PhaseGroup.seeds(query={"page" : 1, "perPage" : 1}).select(ds.SeedConnection.pageInfo.select(ds.PageInfo.total))
                )
            )
        )
    )

    result = execute(q, session)
    event_dict = {}
    for pg in result['event']['phaseGroups']:
        event_dict[ pg['id']] = {"phase_id" : pg['phase']['id'],
                                 "display_id": pg['displayIdentifier'],
                                 "wave_id" : '', 
                                 "start_time":'',
                                 "tournament_slug" : result['event']['tournament']['slug'],
                                 "tournament_name" : result['event']['tournament']['name'],
                                 "tournament_id" : result['event']['tournament']['id']}
    #print(len(result['event']['phaseGroups'])) 
    return event_dict
#pg['wave']['identifier']
#pg['wave']['startAt']
def getPlayersPhaseGroup(pg_id, pg_dict, ds, session) -> dict:
    q = dsl.dsl_gql(
        dsl.DSLQuery(
            ds.Query.phaseGroup(id=pg_id).select(
                ds.PhaseGroup.wave.select(
                    ds.Wave.identifier,
                    ds.Wave.startAt
                ),
                ds.PhaseGroup.seeds(query={"page":1, "perPage":100}).select(
                    ds.SeedConnection.nodes.select(
                        ds.Seed.entrant.select(
                            ds.Entrant.participants.select(
                                ds.Participant.gamerTag,
                                ds.Participant.user.select(
                                    ds.User.discriminator
                                )
                            )
                        )
                    )
                )
            )
        )
    )
    
    result = execute(q, session)
    player_dict = {}
    pg_dict[pg_id]['wave_id'] = result['phaseGroup']['wave']['identifier']
    pg_dict[pg_id]['start_time'] = result['phaseGroup']['wave']['startAt']
    for player in result['phaseGroup']['seeds']['nodes']:
        entrant = player['entrant']
        if entrant == None:
            continue
        for participant in player['entrant']['participants']:
            tag = participant['gamerTag']
            if participant['user'] != None:
                discriminator = participant['user']['discriminator']
            else:
                print(tag)
                continue
            player_dict[discriminator] = tag

    return player_dict

def addEvents(event_dict, conn):
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS events
        ([event_id] INTEGER PRIMARY KEY, [game] TEXT, [event_name] TEXT)
    ''')

    for event in event_dict:
        c.execute("INSERT or IGNORE INTO events (event_id, game, event_name) values (?, ?, ?)",
                (event, event_dict[event][0], event_dict[event][1]))
    conn.commit()

def addPhaseGroups(event, pg_dict, conn):
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS phasegroups
        ([phasegroup_id] INTEGER PRIMARY KEY, [display] TEXT, [phase_id] INTEGER, [wave] TEXT, 
         [tournament_slug] TEXT, [tournament_name] TEXT, [tournament_id] INTEGER,
         [start_time] DATETIME, [event_id] INTEGER, FOREIGN KEY(event_id) REFERENCES events(event_id))
    ''')

    for group in pg_dict:
        c.execute("INSERT or IGNORE INTO phasegroups (phasegroup_id, display, phase_id,  wave, tournament_slug, tournament_name, tournament_id, start_time, event_id) values (?,?,?,?,?,?,?,?,?)",
        (group, pg_dict[group]["display_id"], pg_dict[group]['phase_id'], 
         pg_dict[group]["wave_id"], pg_dict[group]['tournament_slug'],  pg_dict[group]['tournament_name'],  pg_dict[group]['tournament_id'],
         pg_dict[group]["start_time"], event))

    conn.commit()

def addPlayers(pg, player_dict, conn):
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS players
        ([discriminator] TEXT, [tag] TEXT, [phasegroup_id] INTEGER, FOREIGN KEY(phasegroup_id) REFERENCES phasegroups(phasegroup_id), UNIQUE(discriminator, phasegroup_id))
    ''')

    for player in player_dict:
        c.execute("INSERT OR IGNORE INTO players (discriminator, tag, phasegroup_id) values (?,?,?)",
        (player, player_dict[player], pg))

    conn.commit()


def main():

    conn = create_db()
    transport = RequestsHTTPTransport(url="https://api.start.gg/gql/alpha", headers=gen_headers())

    # Create a GraphQL client using the defined transport
    client = Client(transport=transport, fetch_schema_from_transport=True)

    with client as session:
        assert client.schema is not None
        ds = dsl.DSLSchema(client.schema)
        event_dict = getEvents(session, "tournament/ceotaku-2023")
        event_dict = event_dict | getEvents(session, "tournament/ceotaku-2023-community-events")
        addEvents(event_dict, conn)
        all_phasegroups = {}
        for event in event_dict:
            pg_dict = getEventPhaseGroups(event, ds, session)
            player_dict = {}
            for pg in pg_dict:
                time.sleep(.3)
                player_dict = getPlayersPhaseGroup(pg, pg_dict, ds, session)
                addPlayers(pg, player_dict, conn)
            addPhaseGroups(event, pg_dict, conn)
            all_phasegroups = all_phasegroups | pg_dict
        
        print(len(all_phasegroups))
        #addPhaseGroups(all_phasegroups, conn)
        #for pg in all_phasegroups:
        #    player_dict = getPlayersPhaseGroup(pg, all_phasegroups, ds, session)
        #    addPlayers(pg, player_dict, conn)
        #    time.sleep(.5)

if __name__ == "__main__":
    main()
    print(count)