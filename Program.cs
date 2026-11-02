
using System.Collections.Concurrent;
using Microsoft.AspNetCore.SignalR;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSignalR();

var app = builder.Build();

app.MapGet("/", () =>
    "Battle City Multiplayer Server is running!");

app.MapHub<GameHub>("/gameHub");

var port = Environment.GetEnvironmentVariable("PORT") ?? "5113";
app.Run($"http://0.0.0.0:{port}");
public sealed class GameHub : Hub
{
    private const int MaxPlayersPerRoom = 4;
    private const int PlayersRequiredToStart = 2;

    private static readonly ConcurrentDictionary<string, GameRoom>
        Rooms = new();

    public async Task<string> CreateRoom(string playerName)
    {
        playerName = CleanPlayerName(playerName);

        string roomCode;
        GameRoom room;

        do
        {
            roomCode = Guid.NewGuid()
                .ToString("N")[..6]
                .ToUpperInvariant();

            room = new GameRoom(roomCode);
        }
        while (!Rooms.TryAdd(roomCode, room));

        lock (room.SyncRoot)
        {
            room.Players[Context.ConnectionId] = playerName;
        }

        await Groups.AddToGroupAsync(
            Context.ConnectionId, roomCode);

        await Clients.Group(roomCode).SendAsync(
            "PlayerJoined",
            Context.ConnectionId,
            playerName);

        // Send the initial lobby list to Player 1.
        var players = GetPlayers(room);

        await Clients.Group(roomCode).SendAsync(
            "RoomPlayers", players);

        return roomCode;
    }

    public async Task<object[]> JoinRoom(
        string roomCode, string playerName)
    {
        roomCode = (roomCode ?? "").Trim().ToUpperInvariant();
        playerName = CleanPlayerName(playerName);

        if (!Rooms.TryGetValue(roomCode, out var room))
            throw new HubException("Room not found.");

        bool startGame = false;

        lock (room.SyncRoot)
        {
            if (room.Players.ContainsKey(Context.ConnectionId))
                throw new HubException(
                    "You are already in this room.");

            if (room.GameStarted)
                throw new HubException(
                    "This game has already started.");

            if (room.Players.Count >= MaxPlayersPerRoom)
                throw new HubException("This room is full.");

            room.Players[Context.ConnectionId] = playerName;

            if (room.Players.Count >= PlayersRequiredToStart)
            {
                room.GameStarted = true;
                startGame = true;
            }
        }

        await Groups.AddToGroupAsync(
            Context.ConnectionId, roomCode);

        await Clients.Group(roomCode).SendAsync(
            "PlayerJoined",
            Context.ConnectionId,
            playerName);

        var players = GetPlayers(room);

        // Update the lobby on both players' screens.
        await Clients.Group(roomCode).SendAsync(
            "RoomPlayers", players);

        // Start the game for everyone in the room.
        if (startGame)
        {
            await Clients.Group(roomCode).SendAsync(
                "GameStarted");
        }

        return players;
    }

    public async Task SendTankState(
        float x, float y, string direction)
    {
        var room = FindPlayerRoom(Context.ConnectionId);

        if (room is null)
            throw new HubException("Join a room first.");

        await Clients.OthersInGroup(room.Code).SendAsync(
            "TankState",
            Context.ConnectionId,
            x,
            y,
            direction);
    }

    public async Task LeaveRoom()
    {
        var room = FindPlayerRoom(Context.ConnectionId);

        if (room is null)
            return;

        await RemovePlayer(room, Context.ConnectionId);
    }

    public override async Task OnDisconnectedAsync(
        Exception? exception)
    {
        var room = FindPlayerRoom(Context.ConnectionId);

        if (room is not null)
            await RemovePlayer(room, Context.ConnectionId);

        await base.OnDisconnectedAsync(exception);
    }

    private static GameRoom? FindPlayerRoom(
        string connectionId)
    {
        foreach (var room in Rooms.Values)
        {
            lock (room.SyncRoot)
            {
                if (room.Players.ContainsKey(connectionId))
                    return room;
            }
        }

        return null;
    }

    private async Task RemovePlayer(
        GameRoom room, string connectionId)
    {
        string? playerName;

        lock (room.SyncRoot)
        {
            if (!room.Players.Remove(
                connectionId, out playerName))
                return;

            room.GameStarted = false;
        }

        await Groups.RemoveFromGroupAsync(
            connectionId, room.Code);

        await Clients.Group(room.Code).SendAsync(
            "PlayerLeft", connectionId, playerName);

        var players = GetPlayers(room);

        await Clients.Group(room.Code).SendAsync(
            "RoomPlayers", players);

        lock (room.SyncRoot)
        {
            if (room.Players.Count == 0)
                Rooms.TryRemove(room.Code, out _);
        }
    }

    private static object[] GetPlayers(GameRoom room)
    {
        lock (room.SyncRoot)
        {
            return room.Players
                .Select(p => (object)new
                {
                    PlayerName = p.Value
                })
                .ToArray();
        }
    }

    private static string CleanPlayerName(string? name)
    {
        name = (name ?? "").Trim();

        if (name.Length == 0)
            throw new HubException("Enter a player name.");

        if (name.Length > 20)
            name = name[..20];

        return name;
    }

    private sealed class GameRoom(string code)
    {
        public string Code { get; } = code;

        public object SyncRoot { get; } = new();

        public Dictionary<string, string> Players { get; } = new();

        public bool GameStarted { get; set; }
    }
}
