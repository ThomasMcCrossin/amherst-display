# Game Data Structure

This document describes the detailed game data structure in `games/amherst-ramblers.json`.

## Overview

Each game now includes:
- **Scoring plays**: Who scored, assists, time in period, special situations (PP/SH/GWG)
- **Penalties**: Player, infraction, duration, time in period
- **Player stats**: Per-game statistics for every Rambler who played
- All data linked to player IDs from roster system

## File Structure

```json
{
  "team_slug": "amherst-ramblers",
  "team_name": "Amherst Ramblers",
  "team_id": 1,
  "season": "2024-25",
  "season_id": 41,
  "updated_at": "2025-11-09T...",
  "summary": { /* season totals */ },
  "games": [ /* array of game objects */ ]
}
```

## Game Object

Each game in the `games` array contains:

```json
{
  "game_id": "4718",
  "date": "2025-11-08",
  "date_time": "2025-11-08T19:00:00-04:00",
  "opponent": {
    "team_id": 7,
    "team_name": "Pictou County Weeks Crushers",
    "team_code": "PCC"
  },
  "home_game": true,
  "venue": "Amherst Stadium",
  "result": {
    "won": true,
    "ramblers_score": 2,
    "opponent_score": 1,
    "overtime": false,
    "shootout": false,
    "final_score": "2-1"
  },
  "attendance": 917,
  "scoring": [ /* array of goals */ ],
  "penalties": [ /* array of penalties */ ],
  "player_stats": { /* player_id -> stats object */ }
}
```

## Scoring Plays

Each goal includes who scored, assists, and when:

```json
{
  "period": 1,
  "period_name": "1st",
  "time": "8:01",
  "team": "amherst-ramblers",
  "scorer": {
    "player_id": "mhl-3545",
    "name": "Ty Peddigrew",
    "number": 7,
    "position": "C"
  },
  "assists": [
    {
      "player_id": "mhl-3621",
      "name": "Owen Aura",
      "number": 25
    },
    {
      "player_id": "mhl-3598",
      "name": "Anthony Gaudet",
      "number": 11
    }
  ],
  "power_play": false,
  "short_handed": false,
  "game_winning": false,
  "empty_net": false
}
```

## Penalties

Ramblers penalties with player info and infraction details:

```json
{
  "period": 1,
  "period_name": "1st",
  "time": "04:17",
  "player": {
    "player_id": "mhl-3601",
    "name": "Anthony Morin",
    "number": 24,
    "position": "LW"
  },
  "infraction": "High Sticking - Minor",
  "duration": 2,
  "is_bench": false
}
```

## Player Stats (Per Game)

Stats for every Rambler who played, keyed by player_id:

### Skaters

```json
{
  "mhl-3545": {
    "position": "C",
    "goals": 1,
    "assists": 0,
    "points": 1,
    "penalty_minutes": 0,
    "shots": 3,
    "plus_minus": "+1",
    "hits": 2,
    "blocked_shots": 1,
    "faceoff_wins": 8,
    "faceoff_losses": 5
  }
}
```

### Goalies

```json
{
  "mhl-3789": {
    "position": "G",
    "saves": 28,
    "shots_against": 29,
    "goals_against": 1,
    "save_percentage": "0.966",
    "time_on_ice": "60:00"
  }
}
```

## Use Cases for Interactive Displays

### 1. Game Highlights
- Filter `scoring` array for `team: "amherst-ramblers"`
- Show goal scorer with headshot (via player_id)
- Display assists and special situations (PP, SH, GWG)

### 2. Player Spotlight
- Query `player_stats` across multiple games
- Find player's best games by goals/points
- Show game-by-game performance

### 3. Recent Games Widget
- Sort games by date (already sorted, most recent first)
- Show last 5 games with scores
- Highlight star performers from `player_stats`

### 4. Penalty Tracker
- Track players with most penalty minutes
- Show discipline trends over time
- Filter by infraction type

### 5. Scoring Leaders
- Aggregate `scoring` plays by player_id
- Count goals + assists per player
- Show hot/cold streaks

## Example Queries

### Get all goals by a specific player
```javascript
const playerGoals = games.flatMap(game =>
  game.scoring.filter(goal =>
    goal.scorer.player_id === 'mhl-3545' &&
    goal.team === 'amherst-ramblers'
  )
);
```

### Get player's last 5 games stats
```javascript
const playerStats = games
  .slice(0, 5)
  .map(game => ({
    date: game.date,
    opponent: game.opponent.team_code,
    stats: game.player_stats['mhl-3545']
  }));
```

### Find game-winning goals
```javascript
const gwg = games.flatMap(game =>
  game.scoring
    .filter(goal => goal.game_winning && goal.team === 'amherst-ramblers')
    .map(goal => ({
      date: game.date,
      opponent: game.opponent.team_name,
      scorer: goal.scorer
    }))
);
```

### Total penalty minutes per player
```javascript
const pimTotals = {};
games.forEach(game => {
  game.penalties.forEach(penalty => {
    const pid = penalty.player.player_id;
    pimTotals[pid] = (pimTotals[pid] || 0) + penalty.duration;
  });
});
```

## Data Update Frequency

- Updated nightly via GitHub Actions at 3:30am Atlantic
- All completed games processed with full details
- Player IDs automatically linked from roster system
