package dev.proplayer919.hollowduels;

import org.bukkit.Bukkit;
import org.bukkit.ChatColor;
import org.bukkit.Location;
import org.bukkit.Material;
import org.bukkit.command.Command;
import org.bukkit.command.CommandExecutor;
import org.bukkit.command.CommandSender;
import org.bukkit.command.TabCompleter;
import org.bukkit.configuration.file.FileConfiguration;
import org.bukkit.configuration.file.YamlConfiguration;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.block.BlockBreakEvent;
import org.bukkit.event.block.BlockPlaceEvent;
import org.bukkit.event.entity.PlayerDeathEvent;
import org.bukkit.event.inventory.InventoryClickEvent;
import org.bukkit.event.player.PlayerInteractEvent;
import org.bukkit.event.player.PlayerQuitEvent;
import org.bukkit.inventory.Inventory;
import org.bukkit.inventory.ItemStack;
import org.bukkit.inventory.meta.ItemMeta;
import org.bukkit.plugin.java.JavaPlugin;
import org.bukkit.scheduler.BukkitRunnable;
import org.bukkit.util.io.BukkitObjectInputStream;
import org.bukkit.util.io.BukkitObjectOutputStream;
import me.clip.placeholderapi.PlaceholderAPI;
import net.md_5.bungee.api.chat.ClickEvent;
import net.md_5.bungee.api.chat.TextComponent;
import org.bukkit.event.player.AsyncPlayerChatEvent;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.stream.Collectors;

public class DuelPlugin extends JavaPlugin implements Listener {
    private FileConfiguration config;
    private File configFile;
    private Connection dbConnection;
    private Map<UUID, Duel> activeDuels = new ConcurrentHashMap<>();
    private Map<UUID, Location> preDuelLocations = new ConcurrentHashMap<>();
    private Map<UUID, String> pendingInvites = new ConcurrentHashMap<>();
    private Map<UUID, String> chatMode = new ConcurrentHashMap<>();
    private Map<UUID, String> pendingReports = new ConcurrentHashMap<>();
    private Map<String, PreparedStatement> preparedStatements = new ConcurrentHashMap<>();

    @Override
    public void onEnable() {
        saveDefaultConfig();
        configFile = new File(getDataFolder(), "config.yml");
        config = YamlConfiguration.loadConfiguration(configFile);
        try {
            initializeDatabase();
            getServer().getPluginManager().registerEvents(this, this);
            getCommand("duel").setExecutor(new DuelCommand());
            getCommand("duel").setTabCompleter(new DuelTabCompleter());
            getCommand("duels").setExecutor(new DuelsCommand());
            getCommand("duels").setTabCompleter(new DuelsTabCompleter());
            startActionBarTask();
        } catch (SQLException e) {
            getLogger().log(Level.SEVERE, "Failed to initialize plugin", e);
            setEnabled(false);
        }
    }

    @Override
    public void onDisable() {
        try {
            if (dbConnection != null && !dbConnection.isClosed()) {
                dbConnection.close();
            }
        } catch (SQLException e) {
            getLogger().log(Level.SEVERE, "Error closing database connection", e);
        }
    }

    private void initializeDatabase() throws SQLException {
        File dbFile = new File(getDataFolder(), config.getString("database.file", "duels.db"));
        dbConnection = DriverManager.getConnection("jdbc:sqlite:" + dbFile.getPath());
        dbConnection.setAutoCommit(false); // Enable transaction support
        try (Statement stmt = dbConnection.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS kits (name TEXT PRIMARY KEY, items BLOB)");
            stmt.execute("CREATE TABLE IF NOT EXISTS arenas (name TEXT PRIMARY KEY, pos1_x DOUBLE, pos1_y DOUBLE, pos1_z DOUBLE, pos2_x DOUBLE, pos2_y DOUBLE, pos2_z DOUBLE, spawn1_x DOUBLE, spawn1_y DOUBLE, spawn1_z DOUBLE, spawn2_x DOUBLE, spawn2_y DOUBLE, spawn2_z DOUBLE, world TEXT, allowed_kits TEXT)");
            stmt.execute("CREATE TABLE IF NOT EXISTS stats (player_uuid TEXT PRIMARY KEY, wins INTEGER, losses INTEGER, win_streak INTEGER, best_win_streak INTEGER, games_played INTEGER, playtime INTEGER)");
            stmt.execute("CREATE TABLE IF NOT EXISTS duels (duel_id TEXT PRIMARY KEY, player1_uuid TEXT, player2_uuid TEXT, kit TEXT, arena TEXT, start_time INTEGER, end_time INTEGER, winner_uuid TEXT, damage_dealt_p1 REAL, damage_dealt_p2 REAL, player1_inventory BLOB, player2_inventory BLOB)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_stats_uuid ON stats(player_uuid)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_duels_id ON duels(duel_id)");
            dbConnection.commit();
        } catch (SQLException e) {
            dbConnection.rollback();
            throw e;
        }

        // Cache PreparedStatements
        preparedStatements.put("select_kit", dbConnection.prepareStatement("SELECT items FROM kits WHERE name = ?"));
        preparedStatements.put("insert_kit", dbConnection.prepareStatement("INSERT INTO kits (name, items) VALUES (?, ?)"));
        preparedStatements.put("update_kit", dbConnection.prepareStatement("UPDATE kits SET items = ? WHERE name = ?"));
        preparedStatements.put("select_arena", dbConnection.prepareStatement("SELECT pos1_x, pos1_y, pos1_z, pos2_x, pos2_y, pos2_z, world FROM arenas WHERE name = ?"));
        preparedStatements.put("insert_arena", dbConnection.prepareStatement("INSERT INTO arenas (name, allowed_kits) VALUES (?, ?)"));
        preparedStatements.put("update_arena_pos", dbConnection.prepareStatement("UPDATE arenas SET %s_x = ?, %s_y = ?, %s_z = ?, world = ? WHERE name = ?"));
        preparedStatements.put("update_arena_kits", dbConnection.prepareStatement("UPDATE arenas SET allowed_kits = ? WHERE name = ?"));
        preparedStatements.put("select_stats", dbConnection.prepareStatement("SELECT * FROM stats WHERE player_uuid = ?"));
        preparedStatements.put("update_stats", dbConnection.prepareStatement(
                "INSERT OR REPLACE INTO stats (player_uuid, wins, losses, win_streak, best_win_streak, games_played, playtime) " +
                "VALUES (?, COALESCE((SELECT wins FROM stats WHERE player_uuid = ?), 0) + ?, " +
                "COALESCE((SELECT losses FROM stats WHERE player_uuid = ?), 0) + ?, " +
                "?, GREATEST(COALESCE((SELECT best_win_streak FROM stats WHERE player_uuid = ?), 0), ?), " +
                "COALESCE((SELECT games_played FROM stats WHERE player_uuid = ?), 0) + 1, " +
                "COALESCE((SELECT playtime FROM stats WHERE player_uuid = ?), 0) + ?)"));
        preparedStatements.put("insert_duel", dbConnection.prepareStatement(
                "INSERT INTO duels (duel_id, player1_uuid, player2_uuid, kit, arena, start_time, end_time, winner_uuid, damage_dealt_p1, damage_dealt_p2, player1_inventory, player2_inventory) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"));
        preparedStatements.put("select_duel_inv", dbConnection.prepareStatement("SELECT player1_uuid, player1_inventory, player2_uuid, player2_inventory FROM duels WHERE duel_id = ?"));
        preparedStatements.put("select_kits", dbConnection.prepareStatement("SELECT name FROM kits"));
        preparedStatements.put("select_arenas", dbConnection.prepareStatement("SELECT name, allowed_kits FROM arenas"));
        preparedStatements.put("select_duel_ids", dbConnection.prepareStatement("SELECT duel_id FROM duels"));
    }

    private void saveConfigFile() {
        try {
            config.save(configFile);
        } catch (IOException e) {
            getLogger().log(Level.SEVERE, "Failed to save config", e);
        }
    }

    private void startActionBarTask() {
        new BukkitRunnable() {
            @Override
            public void run() {
                for (Duel duel : activeDuels.values()) {
                    Player p1 = duel.player1;
                    Player p2 = duel.player2;
                    if (p1 != null && p2 != null && p1.isOnline() && p2.isOnline()) {
                        sendActionBar(p1, getHealthHearts(p2));
                        sendActionBar(p2, getHealthHearts(p1));
                    }
                }
            }
        }.runTaskTimer(this, 0L, config.getLong("actionbar.update-interval", 10L));
    }

    private String getHealthHearts(Player player) {
        double health = player.getHealth();
        int fullHearts = (int) (health / 2);
        int halfHearts = (health % 2 >= 0.5) ? 1 : 0;
        StringBuilder hearts = new StringBuilder();
        for (int i = 0; i < fullHearts; i++) hearts.append(config.getString("actionbar.full-heart", ChatColor.RED + "❤"));
        if (halfHearts > 0) hearts.append(config.getString("actionbar.half-heart", ChatColor.YELLOW + "❤"));
        return config.getString("actionbar.message", "Opponent Health: ") + hearts;
    }

    private void sendActionBar(Player player, String message) {
        player.sendActionBar(message);
    }

    private class Duel {
        UUID duelId;
        Player player1, player2;
        String kit, arena;
        long startTime;
        double damageDealtP1, damageDealtP2;

        Duel(Player player1, Player player2, String kit, String arena) {
            this.duelId = UUID.randomUUID();
            this.player1 = player1;
            this.player2 = player2;
            this.kit = kit;
            this.arena = arena;
            this.startTime = System.currentTimeMillis();
        }
    }

    private boolean isInArenaBounds(Location loc, String arenaName) {
        try {
            PreparedStatement stmt = preparedStatements.get("select_arena");
            stmt.setString(1, arenaName);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    String world = rs.getString("world");
                    if (!loc.getWorld().getName().equals(world)) return false;
                    double x1 = rs.getDouble("pos1_x"), y1 = rs.getDouble("pos1_y"), z1 = rs.getDouble("pos1_z");
                    double x2 = rs.getDouble("pos2_x"), y2 = rs.getDouble("pos2_y"), z2 = rs.getDouble("pos2_z");
                    double minX = Math.min(x1, x2), maxX = Math.max(x1, x2);
                    double minY = Math.min(y1, y2), maxY = Math.max(y1, y2);
                    double minZ = Math.min(z1, z2), maxZ = Math.max(z1, z2);
                    return loc.getX() >= minX && loc.getX() <= maxX &&
                           loc.getY() >= minY && loc.getY() <= maxY &&
                           loc.getZ() >= minZ && loc.getZ() <= maxZ;
                }
            }
        } catch (SQLException e) {
            getLogger().log(Level.WARNING, "Error checking arena bounds for " + arenaName, e);
        }
        return false;
    }

    private void endDuel(Duel duel, Player winner, Player loser) {
        try {
            long endTime = System.currentTimeMillis();
            activeDuels.remove(duel.player1.getUniqueId());
            activeDuels.remove(duel.player2.getUniqueId());
            updateStats(winner, true, endTime - duel.startTime);
            updateStats(loser, false, endTime - duel.startTime);
            saveDuel(duel, winner.getUniqueId().toString(), duel.player1.getInventory().getContents(), duel.player2.getInventory().getContents());
            sendDuelSummary(duel, winner, loser);
            teleportBack(duel.player1);
            teleportBack(duel.player2);
            dbConnection.commit();
        } catch (SQLException e) {
            try {
                dbConnection.rollback();
            } catch (SQLException rollbackEx) {
                getLogger().log(Level.SEVERE, "Failed to rollback transaction", rollbackEx);
            }
            getLogger().log(Level.SEVERE, "Error ending duel", e);
        }
    }

    private void updateStats(Player player, boolean won, long playtime) {
        try {
            PreparedStatement stmt = preparedStatements.get("update_stats");
            String uuid = player.getUniqueId().toString();
            int winStreak = won ? getWinStreak(player) + 1 : 0;
            stmt.setString(1, uuid);
            stmt.setString(2, uuid);
            stmt.setInt(3, won ? 1 : 0);
            stmt.setString(4, uuid);
            stmt.setInt(5, won ? 0 : 1);
            stmt.setInt(6, winStreak);
            stmt.setString(7, uuid);
            stmt.setInt(8, winStreak);
            stmt.setString(9, uuid);
            stmt.setLong(10, playtime / 1000);
            stmt.addBatch();
            stmt.executeBatch();
            dbConnection.commit();
        } catch (SQLException e) {
            try {
                dbConnection.rollback();
            } catch (SQLException rollbackEx) {
                getLogger().log(Level.SEVERE, "Failed to rollback stats update", rollbackEx);
            }
            getLogger().log(Level.WARNING, "Error updating stats for " + player.getName(), e);
        }
    }

    private int getWinStreak(Player player) {
        try {
            PreparedStatement stmt = preparedStatements.get("select_stats");
            stmt.setString(1, player.getUniqueId().toString());
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) return rs.getInt("win_streak");
            }
        } catch (SQLException e) {
            getLogger().log(Level.WARNING, "Error fetching win streak for " + player.getName(), e);
        }
        return 0;
    }

    private void saveDuel(Duel duel, String winnerUuid, ItemStack[] player1Inventory, ItemStack[] player2Inventory) {
        try {
            PreparedStatement stmt = preparedStatements.get("insert_duel");
            stmt.setString(1, duel.duelId.toString());
            stmt.setString(2, duel.player1.getUniqueId().toString());
            stmt.setString(3, duel.player2.getUniqueId().toString());
            stmt.setString(4, duel.kit);
            stmt.setString(5, duel.arena);
            stmt.setLong(6, duel.startTime);
            stmt.setLong(7, System.currentTimeMillis());
            stmt.setString(8, winnerUuid);
            stmt.setDouble(9, duel.damageDealtP1);
            stmt.setDouble(10, duel.damageDealtP2);
            stmt.setBytes(11, serializeItems(player1Inventory));
            stmt.setBytes(12, serializeItems(player2Inventory));
            stmt.executeUpdate();
            dbConnection.commit();
        } catch (SQLException e) {
            try {
                dbConnection.rollback();
            } catch (SQLException rollbackEx) {
                getLogger().log(Level.SEVERE, "Failed to rollback duel save", rollbackEx);
            }
            getLogger().log(Level.WARNING, "Error saving duel " + duel.duelId, e);
        }
    }

    private void sendDuelSummary(Duel duel, Player winner, Player loser) {
        String summaryFormat = config.getString("messages.duel-summary", "&aDuel Summary: &eWinner: %s &7| Damage Dealt: %.2f &7| Damage Received: %.2f");
        TextComponent summary = new TextComponent(ChatColor.translateAlternateColorCodes('&', String.format(summaryFormat, winner.getName(), duel.damageDealtP1, duel.damageDealtP2)));
        TextComponent viewInv = new TextComponent(ChatColor.translateAlternateColorCodes('&', config.getString("messages.view-inventory", "&9 [View Inventory]")));
        viewInv.setClickEvent(new ClickEvent(ClickEvent.Action.RUN_COMMAND, "/duel viewinv " + duel.duelId));
        TextComponent report = new TextComponent(ChatColor.translateAlternateColorCodes('&', config.getString("messages.report", "&c [Report]")));
        report.setClickEvent(new ClickEvent(ClickEvent.Action.RUN_COMMAND, "/duel report " + duel.duelId));
        winner.spigot().sendMessage(summary, viewInv, report);
        loser.spigot().sendMessage(summary, viewInv, report);
    }

    private void teleportBack(Player player) {
        Location loc = preDuelLocations.remove(player.getUniqueId());
        if (loc != null) {
            player.teleport(loc);
            player.getInventory().clear();
        } else {
            player.sendMessage(ChatColor.translateAlternateColorCodes('&', config.getString("messages.error-no-prelocation", "&cError: No pre-duel location found!")));
        }
    }

    private byte[] serializeItems(ItemStack[] items) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             BukkitObjectOutputStream oos = new BukkitObjectOutputStream(baos)) {
            oos.writeObject(items);
            return baos.toByteArray();
        } catch (IOException e) {
            getLogger().log(Level.WARNING, "Error serializing items", e);
            return new byte[0];
        }
    }

    private ItemStack[] deserializeItems(byte[] data) {
        if (data == null || data.length == 0) return new ItemStack[0];
        try (ByteArrayInputStream bais = new ByteArrayInputStream(data);
             BukkitObjectInputStream ois = new BukkitObjectInputStream(bais)) {
            return (ItemStack[]) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            getLogger().log(Level.WARNING, "Error deserializing items", e);
            return new ItemStack[0];
        }
    }

    @EventHandler
    public void onPlayerDeath(PlayerDeathEvent event) {
        Player loser = event.getEntity();
        Duel duel = activeDuels.get(loser.getUniqueId());
        if (duel != null) {
            Player winner = (duel.player1 == loser) ? duel.player2 : duel.player1;
            if (winner != null && winner.isOnline()) {
                endDuel(duel, winner, loser);
            } else {
                getLogger().log(Level.WARNING, "Winner is offline or null for duel " + duel.duelId);
            }
        }
    }

    @EventHandler
    public void onPlayerQuit(PlayerQuitEvent event) {
        Player player = event.getPlayer();
        Duel duel = activeDuels.get(player.getUniqueId());
        if (duel != null) {
            Player winner = (duel.player1 == player) ? duel.player2 : duel.player1;
            if (winner != null && winner.isOnline()) {
                endDuel(duel, winner, player);
            } else {
                getLogger().log(Level.WARNING, "Winner is offline or null for duel " + duel.duelId);
            }
        }
    }

    @EventHandler
    public void onBlockPlace(BlockPlaceEvent event) {
        Duel duel = activeDuels.get(event.getPlayer().getUniqueId());
        if (duel != null && !isInArenaBounds(event.getBlock().getLocation(), duel.arena)) {
            event.setCancelled(true);
            event.getPlayer().sendMessage(ChatColor.translateAlternateColorCodes('&', config.getString("messages.no-interact-outside-arena", "&cYou cannot place blocks outside the arena!")));
        }
    }

    @EventHandler
    public void onBlockBreak(BlockBreakEvent event) {
        Duel duel = activeDuels.get(event.getPlayer().getUniqueId());
        if (duel != null && !isInArenaBounds(event.getBlock().getLocation(), duel.arena)) {
            event.setCancelled(true);
            event.getPlayer().sendMessage(ChatColor.translateAlternateColorCodes('&', config.getString("messages.no-interact-outside-arena", "&cYou cannot break blocks outside the arena!")));
        }
    }

    @EventHandler
    public void onPlayerInteract(PlayerInteractEvent event) {
        Duel duel = activeDuels.get(event.getPlayer().getUniqueId());
        if (duel != null && event.getClickedBlock() != null && !isInArenaBounds(event.getClickedBlock().getLocation(), duel.arena)) {
            event.setCancelled(true);
            event.getPlayer().sendMessage(ChatColor.translateAlternateColorCodes('&', config.getString("messages.no-interact-outside-arena", "&cYou cannot interact outside the arena!")));
        }
    }

    @EventHandler
    public void onPlayerChat(AsyncPlayerChatEvent event) {
        Player sender = event.getPlayer();
        Duel duel = activeDuels.get(sender.getUniqueId());
        if (duel != null && !chatMode.getOrDefault(sender.getUniqueId(), "opponent").equals("all")) {
            event.setCancelled(true);
            Player opponent = (duel.player1 == sender) ? duel.player2 : duel.player1;
            if (opponent != null && opponent.isOnline()) {
                String format = config.getString("messages.chat-format", "&7[%s] %s");
                String message = ChatColor.translateAlternateColorCodes('&', String.format(format, sender.getName(), event.getMessage()));
                opponent.sendMessage(message);
                sender.sendMessage(message);
            }
        }
    }

    @EventHandler
    public void onInventoryClick(InventoryClickEvent event) {
        Player player = (Player) event.getWhoClicked();
        String title = event.getView().getTitle();
        if (title.equals(config.getString("gui.kit-gui-title", "Select Kit")) || title.equals(config.getString("gui.report-gui-title", "Report Duel")) || title.startsWith(config.getString("gui.inventory-gui-title", "Duel Inventory: "))) {
            event.setCancelled(true);
            ItemStack item = event.getCurrentItem();
            if (item == null || !item.hasItemMeta()) return;

            if (title.equals(config.getString("gui.kit-gui-title", "Select Kit"))) {
                String kitName = ChatColor.stripColor(item.getItemMeta().getDisplayName());
                player.closeInventory();
                String targetName = pendingInvites.remove(player.getUniqueId());
                if (targetName != null) {
                    Player target = getServer().getPlayer(targetName);
                    if (target != null && target.isOnline()) {
                        startDuel(player, target, kitName);
                    } else {
                        player.sendMessage(ChatColor.translateAlternateColorCodes('&', config.getString("messages.target-offline", "&cTarget player is offline!")));
                    }
                }
            } else if (title.equals(config.getString("gui.report-gui-title", "Report Duel"))) {
                String category = ChatColor.stripColor(item.getItemMeta().getDisplayName());
                String duelId = pendingReports.remove(player.getUniqueId());
                if (duelId != null) {
                    sendReport(duelId, player.getName(), category);
                    player.sendMessage(ChatColor.translateAlternateColorCodes('&', config.getString("messages.report-submitted", "&aReport submitted for duel %s").replace("%s", duelId)));
                    player.closeInventory();
                }
            }
        }
    }

    private void startDuel(Player player1, Player player2, String kit) {
        String arena = selectRandomArena(kit);
        if (arena == null) {
            player1.sendMessage(ChatColor.translateAlternateColorCodes('&', config.getString("messages.no-arena", "&cNo available arenas for this kit!")));
            return;
        }
        try {
            Duel duel = new Duel(player1, player2, kit, arena);
            activeDuels.put(player1.getUniqueId(), duel);
            activeDuels.put(player2.getUniqueId(), duel);
            preDuelLocations.put(player1.getUniqueId(), player1.getLocation());
            preDuelLocations.put(player2.getUniqueId(), player2.getLocation());
            teleportToArena(player1, player2, arena);
            applyKit(player1, kit);
            applyKit(player2, kit);
            String startMessage = config.getString("messages.duel-start", "&aDuel started with %s using kit %s");
            player1.sendMessage(ChatColor.translateAlternateColorCodes('&', String.format(startMessage, player2.getName(), kit)));
            player2.sendMessage(ChatColor.translateAlternateColorCodes('&', String.format(startMessage, player1.getName(), kit)));
        } catch (SQLException e) {
            getLogger().log(Level.WARNING, "Error starting duel for " + player1.getName() + " vs " + player2.getName(), e);
            player1.sendMessage(ChatColor.translateAlternateColorCodes('&', config.getString("messages.error-duel-start", "&cFailed to start duel!")));
            player2.sendMessage(ChatColor.translateAlternateColorCodes('&', config.getString("messages.error-duel-start", "&cFailed to start duel!")));
        }
    }

    private void teleportToArena(Player player1, Player player2, String arena) throws SQLException {
        PreparedStatement stmt = preparedStatements.get("select_arena");
        stmt.setString(1, arena);
        try (ResultSet rs = stmt.executeQuery()) {
            if (rs.next()) {
                Location spawn1 = new Location(
                        Bukkit.getWorld(rs.getString("world")),
                        rs.getDouble("spawn1_x"),
                        rs.getDouble("spawn1_y"),
                        rs.getDouble("spawn1_z")
                );
                Location spawn2 = new Location(
                        Bukkit.getWorld(rs.getString("world")),
                        rs.getDouble("spawn2_x"),
                        rs.getDouble("spawn2_y"),
                        rs.getDouble("spawn2_z")
                );
                player1.teleport(spawn1);
                player2.teleport(spawn2);
            } else {
                throw new SQLException("Arena " + arena + " not found");
            }
        }
    }

    private String selectRandomArena(String kit) {
        try {
            PreparedStatement stmt = preparedStatements.get("select_arenas");
            try (ResultSet rs = stmt.executeQuery()) {
                List<String> validArenas = new ArrayList<>();
                while (rs.next()) {
                    String allowedKits = rs.getString("allowed_kits");
                    if (allowedKits != null && Arrays.asList(allowedKits.split(",")).contains(kit)) {
                        validArenas.add(rs.getString("name"));
                    }
                }
                return validArenas.isEmpty() ? null : validArenas.get(new Random().nextInt(validArenas.size()));
            }
        } catch (SQLException e) {
            getLogger().log(Level.WARNING, "Error selecting random arena for kit " + kit, e);
            return null;
        }
    }

    private void applyKit(Player player, String kit) {
        try {
            PreparedStatement stmt = preparedStatements.get("select_kit");
            stmt.setString(1, kit);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    ItemStack[] items = deserializeItems(rs.getBytes("items"));
                    player.getInventory().setContents(items);
                } else {
                    player.sendMessage(ChatColor.translateAlternateColorCodes('&', config.getString("messages.kit-not-found", "&cKit %s not found!").replace("%s", kit)));
                }
            }
        } catch (SQLException e) {
            getLogger().log(Level.WARNING, "Error applying kit " + kit + " to " + player.getName(), e);
            player.sendMessage(ChatColor.translateAlternateColorCodes('&', config.getString("messages.error-apply-kit", "&cFailed to apply kit!")));
        }
    }

    private class DuelCommand implements CommandExecutor {
        @Override
        public boolean onCommand(CommandSender sender, Command command, String label, String[] args) {
            if (!(sender instanceof Player)) {
                sender.sendMessage(ChatColor.translateAlternateColorCodes('&', config.getString("messages.console-error", "&cThis command can only be used by players!")));
                return true;
            }
            Player player = (Player) sender;

            if (args.length == 0) {
                player.sendMessage(ChatColor.translateAlternateColorCodes('&', config.getString("messages.duel-usage", "&cUsage: /duel <player|leave|chatall|viewinv|report>")));
                return true;
            }

            if (args[0].equalsIgnoreCase("leave")) {
                Duel duel = activeDuels.get(player.getUniqueId());
                if (duel != null) {
                    Player winner = (duel.player1 == player) ? duel.player2 : duel.player1;
                    if (winner != null && winner.isOnline()) {
                        endDuel(duel, winner, player);
                        player.sendMessage(ChatColor.translateAlternateColorCodes('&', config.getString("messages.forfeit", "&cYou forfeited the duel!")));
                    } else {
                        player.sendMessage(ChatColor.translateAlternateColorCodes('&', config.getString("messages.error-opponent-offline", "&cOpponent is offline!")));
                    }
                } else {
                    player.sendMessage(ChatColor.translateAlternateColorCodes('&', config.getString("messages.not-in-duel", "&cYou are not in a duel!")));
                }
                return true;
            }

            if (args[0].equalsIgnoreCase("chatall")) {
                chatMode.put(player.getUniqueId(), "all");
                player.sendMessage(ChatColor.translateAlternateColorCodes('&', config.getString("messages.chatall-enabled", "&aNow seeing all chat messages.")));
                return true;
            }

            if (args[0].equalsIgnoreCase("viewinv") && args.length == 2) {
                viewDuelInventory(player, args[1]);
                return true;
            }

            if (args[0].equalsIgnoreCase("report") && args.length == 2) {
                openReportGui(player, args[1]);
                return true;
            }

            Player target = getServer().getPlayer(args[0]);
            if (target == null || !target.isOnline()) {
                player.sendMessage(ChatColor.translateAlternateColorCodes('&', config.getString("messages.target-offline", "&cInvalid or offline player!")));
                return true;
            }
            if (target == player) {
                player.sendMessage(ChatColor.translateAlternateColorCodes('&', config.getString("messages.no-self-duel", "&cYou cannot duel yourself!")));
                return true;
            }

            pendingInvites.put(target.getUniqueId(), player.getName());
            Inventory kitGui = Bukkit.createInventory(null, config.getInt("gui.kit-gui-size", 27), config.getString("gui.kit-gui-title", "Select Kit"));
            try {
                PreparedStatement stmt = preparedStatements.get("select_kits");
                try (ResultSet rs = stmt.executeQuery()) {
                    int slot = 0;
                    while (rs.next() && slot < kitGui.getSize()) {
                        String kitName = rs.getString("name");
                        ItemStack item = new ItemStack(Material.valueOf(config.getString("gui.kit-item", "DIAMOND_SWORD")));
                        ItemMeta meta = item.getItemMeta();
                        meta.setDisplayName(ChatColor.translateAlternateColorCodes('&', config.getString("gui.kit-item-name", "&e%s").replace("%s", kitName)));
                        if (!hasAvailableArena(kitName)) {
                            meta.setLore(Arrays.asList(ChatColor.translateAlternateColorCodes('&', config.getString("gui.no-arena-lore", "&cNo available arenas!"))));
                        }
                        item.setItemMeta(meta);
                        kitGui.setItem(slot++, item);
                    }
                }
            } catch (SQLException e) {
                getLogger().log(Level.WARNING, "Error fetching kits for GUI", e);
                player.sendMessage(ChatColor.translateAlternateColorCodes('&', config.getString("messages.error-fetch-kits", "&cFailed to load kits!")));
            }
            player.openInventory(kitGui);
            return true;
        }

        private boolean hasAvailableArena(String kit) {
            try {
                PreparedStatement stmt = preparedStatements.get("select_arenas");
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        String allowedKits = rs.getString("allowed_kits");
                        if (allowedKits != null && Arrays.asList(allowedKits.split(",")).contains(kit)) {
                            return true;
                        }
                    }
                }
            } catch (SQLException e) {
                getLogger().log(Level.WARNING, "Error checking arena availability for kit " + kit, e);
            }
            return false;
        }

        private void viewDuelInventory(Player player, String duelId) {
            try {
                PreparedStatement stmt = preparedStatements.get("select_duel_inv");
                stmt.setString(1, duelId);
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        Inventory inv = Bukkit.createInventory(null, 54, config.getString("gui.inventory-gui-title", "Duel Inventory: ") + duelId);
                        ItemStack[] p1Items = deserializeItems(rs.getBytes("player1_inventory"));
                        ItemStack[] p2Items = deserializeItems(rs.getBytes("player2_inventory"));
                        String p1Uuid = rs.getString("player1_uuid");
                        String p2Uuid = rs.getString("player2_uuid");
                        Player p1 = Bukkit.getPlayer(UUID.fromString(p1Uuid));
                        Player p2 = Bukkit.getPlayer(UUID.fromString(p2Uuid));
                        String p1Name = p1 != null ? p1.getName() : p1Uuid;
                        String p2Name = p2 != null ? p2.getName() : p2Uuid;

                        // Create glass pane borders
                        ItemStack border = new ItemStack(Material.valueOf(config.getString("gui.border-material", "GRAY_STAINED_GLASS_PANE")));
                        ItemMeta borderMeta = border.getItemMeta();
                        borderMeta.setDisplayName(ChatColor.RESET + "");
                        border.setItemMeta(borderMeta);

                        // Set borders for the entire inventory
                        for (int i = 0; i < 54; i++) {
                            if (i < 9 || i >= 45 || i % 9 == 0 || i % 9 == 8) {
                                inv.setItem(i, border);
                            }
                        }

                        // Player 1 inventory (rows 2-3, slots 10-25)
                        for (int i = 0; i < 16 && i < p1Items.length; i++) {
                            inv.setItem(10 + i, p1Items[i]);
                        }
                        // Player 1 armor (slots 37-40)
                        ItemStack[] p1Armor = p1 != null ? p1.getInventory().getArmorContents() : new ItemStack[4];
                        for (int i = 0; i < 4 && i < p1Armor.length; i++) {
                            inv.setItem(37 + i, p1Armor[i]);
                        }
                        // Player 1 offhand (slot 41)
                        inv.setItem(41, p1 != null ? p1.getInventory().getItemInOffHand() : null);

                        // Player 2 inventory (rows 4-5, slots 28-43)
                        for (int i = 0; i < 16 && i < p2Items.length; i++) {
                            inv.setItem(28 + i, p2Items[i]);
                        }
                        // Player 2 armor (slots 46-49)
                        ItemStack[] p2Armor = p2 != null ? p2.getInventory().getArmorContents() : new ItemStack[4];
                        for (int i = 0; i < 4 && i < p2Armor.length; i++) {
                            inv.setItem(46 + i, p2Armor[i]);
                        }
                        // Player 2 offhand (slot 50)
                        inv.setItem(50, p2 != null ? p2.getInventory().getItemInOffHand() : null);

                        player.openInventory(inv);
                    } else {
                        player.sendMessage(ChatColor.translateAlternateColorCodes('&', config.getString("messages.duel-not-found", "&cDuel ID not found!")));
                    }
                }
            } catch (SQLException e) {
                getLogger().log(Level.WARNING, "Error viewing duel inventory " + duelId, e);
                player.sendMessage(ChatColor.translateAlternateColorCodes('&', config.getString("messages.error-view-inventory", "&cFailed to view duel inventory!")));
            }
        }

        private void openReportGui(Player player, String duelId) {
            Inventory reportGui = Bukkit.createInventory(null, config.getInt("gui.report-gui-size", 27), config.getString("gui.report-gui-title", "Report Duel"));
            List<String> categories = config.getStringList("report-categories");
            for (int i = 0; i < categories.size() && i < reportGui.getSize(); i++) {
                ItemStack item = new ItemStack(Material.valueOf(config.getString("gui.report-item", "PAPER")));
                ItemMeta meta = item.getItemMeta();
                meta.setDisplayName(ChatColor.translateAlternateColorCodes('&', config.getString("gui.report-item-name", "&e%s").replace("%s", categories.get(i))));
                item.setItemMeta(meta);
                reportGui.setItem(i, item);
            }
            pendingReports.put(player.getUniqueId(), duelId);
            player.openInventory(reportGui);
        }

        private void sendReport(String duelId, String reporter, String category) {
            String webhookUrl = config.getString("discord.webhook-url");
            if (webhookUrl == null || webhookUrl.isEmpty()) {
                getLogger().log(Level.WARNING, "Discord webhook URL not configured");
                return;
            }
            try {
                URL url = new URL(webhookUrl);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("POST");
                conn.setRequestProperty("Content-Type", "application/json");
                conn.setDoOutput(true);
                String json = config.getString("discord.webhook-message", "{\"content\": \"Report for duel %s by %s: %s\"}")
                        .replace("%s", duelId)
                        .replace("%s", reporter)
                        .replace("%s", category);
                try (OutputStream os = conn.getOutputStream()) {
                    os.write(json.getBytes());
                }
                int responseCode = conn.getResponseCode();
                if (responseCode != 200 && responseCode != 204) {
                    getLogger().log(Level.WARNING, "Failed to send webhook for duel " + duelId + ", response code: " + responseCode);
                }
            } catch (IOException e) {
                getLogger().log(Level.WARNING, "Error sending webhook for duel " + duelId, e);
            }
        }
    }

    private class DuelsCommand implements CommandExecutor {
        @Override
        public boolean onCommand(CommandSender sender, Command command, String label, String[] args) {
            if (!(sender instanceof Player)) {
                sender.sendMessage(ChatColor.translateAlternateColorCodes('&', config.getString("messages.console-error", "&cThis command can only be used by players!")));
                return true;
            }
            Player player = (Player) sender;

            if (args.length == 0) {
                player.sendMessage(ChatColor.translateAlternateColorCodes('&', config.getString("messages.duels-usage", "&cUsage: /duels <createkit|editkit|createarena|setpos1|setpos2|setspawn1|setspawn2|setkitarena|viewduelinv|reload>")));
                return true;
            }

            try {
                if (args[0].equalsIgnoreCase("createkit") && args.length == 2) {
                    PreparedStatement stmt = preparedStatements.get("insert_kit");
                    stmt.setString(1, args[1]);
                    stmt.setBytes(2, serializeItems(player.getInventory().getContents()));
                    stmt.executeUpdate();
                    dbConnection.commit();
                    player.sendMessage(ChatColor.translateAlternateColorCodes('&', config.getString("messages.kit-created", "&aKit %s created!").replace("%s", args[1])));
                    return true;
                }

                if (args[0].equalsIgnoreCase("editkit") && args.length == 2) {
                    PreparedStatement stmt = preparedStatements.get("update_kit");
                    stmt.setBytes(1, serializeItems(player.getInventory().getContents()));
                    stmt.setString(2, args[1]);
                    int rows = stmt.executeUpdate();
                    dbConnection.commit();
                    if (rows > 0) {
                        player.sendMessage(ChatColor.translateAlternateColorCodes('&', config.getString("messages.kit-updated", "&aKit %s updated!").replace("%s", args[1])));
                    } else {
                        player.sendMessage(ChatColor.translateAlternateColorCodes('&', config.getString("messages.kit-not-found", "&cKit %s not found!").replace("%s", args[1])));
                    }
                    return true;
                }

                if (args[0].equalsIgnoreCase("createarena") && args.length == 2) {
                    PreparedStatement stmt = preparedStatements.get("insert_arena");
                    stmt.setString(1, args[1]);
                    stmt.setString(2, "");
                    stmt.executeUpdate();
                    dbConnection.commit();
                    player.sendMessage(ChatColor.translateAlternateColorCodes('&', config.getString("messages.arena-created", "&aArena %s created!").replace("%s", args[1])));
                    return true;
                }

                if (args[0].equalsIgnoreCase("setpos1") && args.length == 2) {
                    updateArenaPosition(player, args[1], "pos1");
                    return true;
                }

                if (args[0].equalsIgnoreCase("setpos2") && args.length == 2) {
                    updateArenaPosition(player, args[1], "pos2");
                    return true;
                }

                if (args[0].equalsIgnoreCase("setspawn1") && args.length == 2) {
                    updateArenaPosition(player, args[1], "spawn1");
                    return true;
                }

                if (args[0].equalsIgnoreCase("setspawn2") && args.length == 2) {
                    updateArenaPosition(player, args[1], "spawn2");
                    return true;
                }

                if (args[0].equalsIgnoreCase("setkitarena") && args.length == 3) {
                    updateArenaKits(args[1], args[2]);
                    player.sendMessage(ChatColor.translateAlternateColorCodes('&', config.getString("messages.kit-arena-set", "&aKit %s added to arena %s").replace("%s", args[2]).replace("%s", args[1])));
                    return true;
                }

                if (args[0].equalsIgnoreCase("viewduelinv") && args.length == 2) {
                    if (player.hasPermission(config.getString("permissions.admin", "duels.admin"))) {
                        new DuelCommand().viewDuelInventory(player, args[1]);
                    } else {
                        player.sendMessage(ChatColor.translateAlternateColorCodes('&', config.getString("messages.no-permission", "&cYou don't have permission!")));
                    }
                    return true;
                }

                if (args[0].equalsIgnoreCase("reload")) {
                    reloadConfig();
                    config = getConfig();
                    player.sendMessage(ChatColor.translateAlternateColorCodes('&', config.getString("messages.config-reloaded", "&aConfig reloaded!")));
                    return true;
                }
            } catch (SQLException e) {
                getLogger().log(Level.WARNING, "Error executing duels command: " + args[0], e);
                player.sendMessage(ChatColor.translateAlternateColorCodes('&', config.getString("messages.error-command", "&cAn error occurred while executing the command!")));
            }
            return false;
        }

        private void updateArenaPosition(Player player, String arena, String pos) throws SQLException {
            PreparedStatement stmt = preparedStatements.get("update_arena_pos");
            stmt = dbConnection.prepareStatement(String.format("UPDATE arenas SET %s_x = ?, %s_y = ?, %s_z = ?, world = ? WHERE name = ?", pos, pos));
            Location loc = player.getLocation();
            stmt.setDouble(1, loc.getX());
            stmt.setDouble(2, loc.getY());
            stmt.setDouble(3, loc.getZ());
            stmt.setString(4, loc.getWorld().getName());
            stmt.setString(5, arena);
            int rows = stmt.executeUpdate();
            dbConnection.commit();
            if (rows > 0) {
                player.sendMessage(ChatColor.translateAlternateColorCodes('&', config.getString("messages.position-set", "&a%s set for arena %s").replace("%s", pos).replace("%s", arena)));
            } else {
                player.sendMessage(ChatColor.translateAlternateColorCodes('&', config.getString("messages.arena-not-found", "&cArena %s not found!").replace("%s", arena)));
            }
        }

        private void updateArenaKits(String arena, String kit) throws SQLException {
            PreparedStatement stmt = preparedStatements.get("select_arenas");
            stmt.setString(1, arena);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    String currentKits = rs.getString("allowed_kits");
                    String updatedKits = currentKits == null || currentKits.isEmpty() ? kit : currentKits + "," + kit;
                    PreparedStatement updateStmt = preparedStatements.get("update_arena_kits");
                    updateStmt.setString(1, updatedKits);
                    updateStmt.setString(2, arena);
                    updateStmt.executeUpdate();
                    dbConnection.commit();
                } else {
                    throw new SQLException("Arena " + arena + " not found");
                }
            }
        }
    }

    private class DuelTabCompleter implements TabCompleter {
        @Override
        public List<String> onTabComplete(CommandSender sender, Command command, String alias, String[] args) {
            List<String> completions = new ArrayList<>();
            if (args.length == 1) {
                completions.addAll(Arrays.asList("leave", "chatall", "viewinv", "report"));
                completions.addAll(Bukkit.getOnlinePlayers().stream()
                        .map(Player::getName)
                        .filter(name -> !name.equals(sender.getName()))
                        .collect(Collectors.toList()));
            } else if (args.length == 2 && (args[0].equalsIgnoreCase("viewinv") || args[0].equalsIgnoreCase("report"))) {
                try {
                    PreparedStatement stmt = preparedStatements.get("select_duel_ids");
                    try (ResultSet rs = stmt.executeQuery()) {
                        while (rs.next()) {
                            completions.add(rs.getString("duel_id"));
                        }
                    }
                } catch (SQLException e) {
                    getLogger().log(Level.WARNING, "Error fetching duel IDs for tab completion", e);
                }
            }
            return completions.stream()
                    .filter(s -> s.toLowerCase().startsWith(args[args.length - 1].toLowerCase()))
                    .collect(Collectors.toList());
        }
    }

    private class DuelsTabCompleter implements TabCompleter {
        @Override
        public List<String> onTabComplete(CommandSender sender, Command command, String alias, String[] args) {
            List<String> completions = new ArrayList<>();
            if (args.length == 1) {
                completions.addAll(Arrays.asList("createkit", "editkit", "createarena", "setpos1", "setpos2", "setspawn1", "setspawn2", "setkitarena", "viewduelinv", "reload"));
            } else if (args.length == 2 && Arrays.asList("editkit", "setpos1", "setpos2", "setspawn1", "setspawn2", "setkitarena").contains(args[0].toLowerCase())) {
                try {
                    PreparedStatement stmt = preparedStatements.get(args[0].equalsIgnoreCase("editkit") ? "select_kits" : "select_arenas");
                    try (ResultSet rs = stmt.executeQuery()) {
                        while (rs.next()) {
                            completions.add(rs.getString("name"));
                        }
                    }
                } catch (SQLException e) {
                    getLogger().log(Level.WARNING, "Error fetching names for tab completion", e);
                }
            } else if (args.length == 2 && args[0].equalsIgnoreCase("viewduelinv")) {
                try {
                    PreparedStatement stmt = preparedStatements.get("select_duel_ids");
                    try (ResultSet rs = stmt.executeQuery()) {
                        while (rs.next()) {
                            completions.add(rs.getString("duel_id"));
                        }
                    }
                } catch (SQLException e) {
                    getLogger().log(Level.WARNING, "Error fetching duel IDs for tab completion", e);
                }
            } else if (args.length == 3 && args[0].equalsIgnoreCase("setkitarena")) {
                try {
                    PreparedStatement stmt = preparedStatements.get("select_kits");
                    try (ResultSet rs = stmt.executeQuery()) {
                        while (rs.next()) {
                            completions.add(rs.getString("name"));
                        }
                    }
                } catch (SQLException e) {
                    getLogger().log(Level.WARNING, "Error fetching kits for tab completion", e);
                }
            }
            return completions.stream()
                    .filter(s -> s.toLowerCase().startsWith(args[args.length - 1].toLowerCase()))
                    .collect(Collectors.toList());
        }
    }

    // PlaceholderAPI Hook
    public String onPlaceholderRequest(Player player, String identifier) {
        if (player == null) return "";
        try {
            PreparedStatement stmt = preparedStatements.get("select_stats");
            stmt.setString(1, player.getUniqueId().toString());
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    switch (identifier.toLowerCase()) {
                        case "wins": return String.valueOf(rs.getInt("wins"));
                        case "losses": return String.valueOf(rs.getInt("losses"));
                        case "win_streak": return String.valueOf(rs.getInt("win_streak"));
                        case "best_win_streak": return String.valueOf(rs.getInt("best_win_streak"));
                        case "games_played": return String.valueOf(rs.getInt("games_played"));
                        case "playtime": return String.valueOf(rs.getInt("playtime"));
                    }
                }
            }
        } catch (SQLException e) {
            getLogger().log(Level.WARNING, "Error fetching placeholder for " + player.getName(), e);
        }
        return "0";
    }
}
