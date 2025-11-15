# convoy_bot.py
# -*- coding: utf-8 -*-

import discord
from discord import app_commands, ui
from discord.ext import commands
import aiohttp
import json
import random
import socket
import asyncio
import os
import time
from datetime import datetime, timezone, timedelta
import math
import logging
from typing import List, Optional, Dict, Union, Any
import string

# --- Configuration Loading & Constants ---
CONFIG_FILE = 'config.json'
LINKED_ACCOUNTS_FILE = 'linked_accounts.json'
INVITE_COUNTS_FILE = 'invite_counts.json'
IP_FILENAME = "ips.txt"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('convoybot')

def load_json_file(filename: str, default: Union[Dict, List] = None) -> Union[Dict, List, None]:
    if default is None:
        default = {}
    if not os.path.exists(filename):
        logger.warning(f"{filename} not found. Creating with default.")
        try:
            with open(filename, 'w') as f:
                json.dump(default, f, indent=4)
            logger.info(f"Created {filename}")
        except IOError as e:
            logger.error(f"Could not create {filename}: {e}")
            return default
        return default
    try:
        with open(filename, 'r') as f:
            return json.load(f)
    except json.JSONDecodeError:
        logger.error(f"{filename} is not valid JSON. Please fix or delete it.")
        return default
    except Exception as e:
        logger.error(f"Failed to load {filename}: {e}")
        return default

def save_json_file(filename: str, data: Union[Dict, List]) -> bool:
    try:
        temp_filename = f"{filename}.tmp"
        with open(temp_filename, 'w') as f:
            json.dump(data, f, indent=4)
        os.replace(temp_filename, filename)
        return True
    except IOError as e:
        logger.error(f"Failed to save {filename}: {e}")
        return False
    except Exception as e:
        logger.error(f"An unexpected error occurred while saving {filename}: {e}")
        if os.path.exists(temp_filename):
            try: os.remove(temp_filename)
            except OSError: pass
        return False

config = load_json_file(CONFIG_FILE)
if not config:
    logger.critical(f"Could not load {CONFIG_FILE}. Exiting.")
    exit(1)

# Update API keys dengan yang baru
config['convoy_api_key'] = "1|B5LzamiPPNA2JaGqBhm7nuA9aFVJDTfbagmKjPBE"
config['client_api_key'] = "1|B5LzamiPPNA2JaGqBhm7nuA9aFVJDTfbagmKjPBE"

# Emoji lookup helper
def Elookup(key: str, default: str = "❓") -> str:
    return config.get('emojis', {}).get(key, default)

# --- Essential Config Validation ---
DISCORD_TOKEN = config.get('discord_token')
CONVOY_API_URL_BASE = config.get('convoy_api_url', '').rstrip('/')
CONVOY_APP_API_KEY = config.get('convoy_api_key')
CONVOY_CLIENT_API_KEY = config.get('client_api_key')
VPS_CREATOR_ROLE_ID = config.get('vps_creator_role_id')
BOT_OWNER_USER_ID = config.get('bot_owner_user_id')
PANEL_BASE_URL = config.get('panel_base_url', CONVOY_API_URL_BASE).rstrip('/')

VPS_LOG_CHANNEL_ID = config.get('channel_ids', {}).get('vps_log')
ADMIN_VPS_APPROVAL_CHANNEL_ID = config.get('channel_ids', {}).get('admin_vps_approval')

CONVOY_APP_API_URL = f"{CONVOY_API_URL_BASE}/api/application"
CONVOY_CLIENT_API_URL = f"{CONVOY_API_URL_BASE}/api/client"

if not all([DISCORD_TOKEN, CONVOY_API_URL_BASE, CONVOY_APP_API_KEY, CONVOY_CLIENT_API_KEY, VPS_CREATOR_ROLE_ID, BOT_OWNER_USER_ID]):
    logger.critical("Missing required fields in config.json")
    exit(1)

try:
    VPS_CREATOR_ROLE_ID = int(VPS_CREATOR_ROLE_ID)
    BOT_OWNER_USER_ID = int(BOT_OWNER_USER_ID)
    if VPS_LOG_CHANNEL_ID: VPS_LOG_CHANNEL_ID = int(VPS_LOG_CHANNEL_ID)
    if ADMIN_VPS_APPROVAL_CHANNEL_ID: ADMIN_VPS_APPROVAL_CHANNEL_ID = int(ADMIN_VPS_APPROVAL_CHANNEL_ID)
except (ValueError, TypeError):
    logger.critical("vps_creator_role_id, bot_owner_user_id, and channel_ids in config.json must be valid integers.")
    exit(1)

if VPS_LOG_CHANNEL_ID is None:
    logger.warning("`channel_ids.vps_log` not set. VPS creation logging will be disabled.")
if ADMIN_VPS_APPROVAL_CHANNEL_ID is None:
    logger.warning("`channel_ids.admin_vps_approval` not set. User VPS creation requests cannot be processed.")

# Reward Toggles & Data
BOOST_REWARDS_ENABLED = config.get('reward_plans_enabled', {}).get('boost', False)
INVITE_REWARDS_ENABLED_GLOBAL = config.get('reward_plans_enabled', {}).get('invite', False)

BOOST_REWARD_TIERS = config.get('boost_reward_tiers', [])
INVITE_REWARD_TIERS = config.get('invite_reward_tiers', [])
PAID_PLANS_DATA = config.get('paid_plans_data', [])

# Defaults from config
DEFAULT_NODE_ID = config.get('defaults', {}).get('node_id')
DEFAULT_TEMPLATE_UUID = config.get('defaults', {}).get('template_uuid')
DEFAULT_USER_SNAPSHOT_LIMIT = config.get('defaults', {}).get('user_snapshot_limit', 1)
DEFAULT_USER_BACKUP_LIMIT = config.get('defaults', {}).get('user_backup_limit', 1)
DEFAULT_ADMIN_SNAPSHOT_LIMIT = config.get('defaults', {}).get('admin_snapshot_limit', 2)
DEFAULT_ADMIN_BACKUP_LIMIT = config.get('defaults', {}).get('admin_backup_limit', 0)
DEFAULT_SERVER_HOSTNAME_SUFFIX = config.get('defaults', {}).get('default_server_hostname_suffix', 'rn-nodes.pro')

NODE_IPS_MAP = config.get('node_ips_map', {})
INVITE_CHECK_DELAY_SECONDS = config.get('invite_check_delay_seconds', 3)

# --- Invite Tracking Data ---
invite_counts: Dict[str, Dict[str, int]] = load_json_file(INVITE_COUNTS_FILE, {})
guild_invite_cache: Dict[int, Dict[str, int]] = {}

# --- Bot Setup ---
intents = discord.Intents.default()
intents.members = True
intents.guilds = True
intents.invites = True

class ConvoyBot(commands.Bot):
    def __init__(self):
        super().__init__(command_prefix="!", intents=intents)
        self.http_session: Optional[aiohttp.ClientSession] = None
        self.linked_accounts: Dict[str, str] = {}

    async def setup_hook(self):
        self.http_session = aiohttp.ClientSession(headers={"User-Agent": "ConvoyDiscordBot/1.1"})
        logger.info("HTTP Session Created")

        self.linked_accounts = load_json_file(LINKED_ACCOUNTS_FILE, {})
        logger.info(f"Loaded {len(self.linked_accounts)} linked accounts.")

        global invite_counts
        invite_counts = load_json_file(INVITE_COUNTS_FILE, {})
        logger.info(f"Loaded invite counts for {len(invite_counts)} guilds.")

        self.tree.add_command(admin_group)
        logger.info("Added admin command group to the tree.")

        try:
            synced_global = await self.tree.sync()
            logger.info(f"Synced {len(synced_global)} application commands globally.")
        except Exception as e:
            logger.error(f"Failed to sync commands on startup: {e}")

        self.loop.create_task(self.cache_invites_periodically())

    async def close(self):
        logger.info("Shutting down RN Nodes bot...")
        await super().close()
        if self.http_session:
            await self.http_session.close()
            logger.info("Closing connection to Panel API...")

    async def cache_invites_periodically(self, interval_seconds=300):
        await self.wait_until_ready()
        while not self.is_closed():
            for guild in self.guilds:
                try:
                    invites = await guild.invites()
                    guild_invite_cache[guild.id] = {invite.code: invite.uses for invite in invites if invite.code and invite.uses is not None}
                except discord.Forbidden:
                    logger.warning(f"No permission to fetch invites for guild {guild.id} ({guild.name})")
                except discord.HTTPException as e:
                    logger.error(f"HTTPException caching invites for guild {guild.id}: {e}")
                except Exception as e:
                    logger.error(f"Unexpected error caching invites for guild {guild.id}: {e}")
            await asyncio.sleep(interval_seconds)

bot = ConvoyBot()

# --- API Jokes ---
api_jokes = [
    "Why did the API break up with the database? It had too many commitment issues! 😂", 
    "Why don't APIs ever get lonely? Because they always have endpoints! 🤝",
    "What do you call a lazy API? RESTful! 😴", 
    "Why was the API key always calm? It knew how to handle requests.🧘",
    "How do APIs stay in shape? By running endpoints! 🏃‍♀️", 
    "Why did the developer bring a ladder to the API meeting? To reach the high-level endpoints! 🪜",
    "What's an API's favorite type of music? Heavy Metal... because it handles a lot of requests! 🤘", 
    "Why was the JSON data always invited to parties? Because it knew how to structure things! 🎉",
    "How does an API apologize? It sends a '418 I'm a teapot' status! 🫖", 
    "What did the API say to the UI? 'You complete me!' ❤️",
] * 5

def get_random_api_joke(): 
    return random.choice(api_jokes)

def get_and_remove_first_ip(filename: str = IP_FILENAME) -> Optional[str]:
    found_ip = None
    lines_to_keep = []
    ip_found_and_skipped = False
    try:
        if not os.path.exists(filename):
            logger.warning(f"IP address file '{filename}' not found. Creating an empty one.")
            with open(filename, 'w') as f:
                f.write("# Add one IP address per line. Lines starting with # are comments.\n")
            return None
            
        with open(filename, 'r') as f:
            all_lines = f.readlines()

        for line in all_lines:
            stripped_line = line.strip()
            if not ip_found_and_skipped:
                if stripped_line and not stripped_line.startswith('#'):
                    found_ip = stripped_line
                    ip_found_and_skipped = True
                else:
                    pass
            else:
                lines_to_keep.append(line)

        if found_ip:
            try:
                with open(filename, 'w') as f:
                    f.writelines(lines_to_keep)
                logger.info(f"Successfully used IP '{found_ip}' and updated '{filename}'.")
            except IOError as e:
                logger.error(f"Error writing back to file '{filename}': {e}")
        else:
             logger.warning(f"No valid IP address found in '{filename}'. File not modified.")
    except FileNotFoundError:
        logger.error(f"IP address file '{filename}' not found (should have been created).")
        return None
    except IOError as e:
        logger.error(f"Error reading/writing file '{filename}': {e}")
        return None
    return found_ip

# --- Helper Functions ---
def generate_compliant_password(length: int = 16) -> str:
    if not (8 <= length <= 50):
        length = 16
    if length < 4:
        length = 8

    lower_chars = "abcdefghijklmnopqrstuvwxyz"
    upper_chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    digit_chars = "0123456789"
    guaranteed_special_char_set = "!@#$%^&*_+-=?"
    all_fill_chars = lower_chars + upper_chars + digit_chars + guaranteed_special_char_set

    password_components = [
        random.choice(lower_chars), random.choice(upper_chars),
        random.choice(digit_chars), random.choice(guaranteed_special_char_set)
    ]
    remaining_length = length - len(password_components)
    if remaining_length > 0:
        password_components.extend(random.choice(all_fill_chars) for _ in range(remaining_length))
    elif remaining_length < 0:
        password_components = password_components[:length]
    random.shuffle(password_components)
    return "".join(password_components)

def check_is_vps_creator(interaction: discord.Interaction) -> bool:
    if not interaction.guild or not isinstance(interaction.user, discord.Member):
        return False
    return any(role.id == VPS_CREATOR_ROLE_ID for role in interaction.user.roles)

def is_vps_creator():
    async def predicate(interaction: discord.Interaction) -> bool:
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await send_error_embed(interaction, "Invalid Context", "This command can only be used within a server.")
            return False
        has_role = any(role.id == VPS_CREATOR_ROLE_ID for role in interaction.user.roles)
        if not has_role:
            role_object = interaction.guild.get_role(VPS_CREATOR_ROLE_ID)
            role_name = f"'{role_object.name}'" if role_object else f"(ID: {VPS_CREATOR_ROLE_ID})"
            await send_error_embed(interaction, f"{Elookup('vps_creator')} Permission Denied", f"Hold up! Only users with the **VPS Creator** role {role_name} can use this command.")
        return has_role
    return app_commands.check(predicate)

def is_bot_owner():
    async def predicate(interaction: discord.Interaction) -> bool:
        is_owner = interaction.user.id == BOT_OWNER_USER_ID
        if not is_owner:
            await send_error_embed(interaction, f"{Elookup('owner')} Permission Denied", "Sorry, this command is reserved for the Bot Owner.")
        return is_owner
    return app_commands.check(predicate)

async def send_embed(
    interaction: discord.Interaction, title: str, description: str, color: discord.Color,
    ephemeral: bool = True, fields: Optional[Dict[str, str]] = None,
    view: Optional[discord.ui.View] = None, add_joke: bool = False
) -> Optional[Union[discord.Message, discord.WebhookMessage]]:
    embed = discord.Embed(title=title, description=description, color=color, timestamp=datetime.now(timezone.utc))
    if fields:
        for name, value in fields.items():
            embed.add_field(name=f"**{name}**", value=str(value)[:1024] if value else "`N/A`", inline=False)
    if add_joke:
        embed.description += f"\n\n*Psst! {get_random_api_joke()}*"

    args = {"embed": embed, "ephemeral": ephemeral}
    if view: args["view"] = view
    sent_message = None
    try:
        if interaction.response.is_done():
            sent_message = await interaction.followup.send(**args)
        else:
            await interaction.response.send_message(**args)
            if not ephemeral: sent_message = await interaction.original_response()
    except discord.errors.InteractionResponded:
        try:
            sent_message = await interaction.followup.send(**args)
        except Exception as followup_error:
            logger.error(f"Failed to send followup after InteractionResponded for '{title}': {followup_error}")
    except discord.errors.NotFound:
        logger.error(f"Interaction not found during send_embed for '{title}'.")
    except Exception as e:
        logger.error(f"Unexpected error during send_embed for '{title}': {e}", exc_info=True)
        try:
            fallback_msg = f"**{title}**\n{description}"
            if interaction.response.is_done(): await interaction.followup.send(fallback_msg, ephemeral=True)
            else: await interaction.response.send_message(fallback_msg, ephemeral=True)
        except Exception as fallback_e:
            logger.error(f"Fallback text message also failed for '{title}': {fallback_e}")
    return sent_message

async def send_error_embed(interaction: discord.Interaction, title: str, description: str, ephemeral: bool = True):
    return await send_embed(interaction, f"{Elookup('error')} Error: {title}", description, discord.Color.red(), ephemeral=ephemeral, add_joke=True)

async def send_success_embed(interaction: discord.Interaction, title: str, description: str, ephemeral: bool = False, view: Optional[discord.ui.View] = None, fields: Optional[Dict[str, str]] = None):
    return await send_embed(interaction, f"{Elookup('success')} Success: {title}", description, discord.Color.green(), ephemeral=ephemeral, view=view, fields=fields)

async def send_info_embed(interaction: discord.Interaction, title: str, description: str, fields: Optional[Dict[str, str]] = None, ephemeral: bool = False, view: Optional[discord.ui.View] = None):
    return await send_embed(interaction, f"{Elookup('info')} {title}", description, discord.Color.blue(), ephemeral=ephemeral, fields=fields, view=view)

# --- Panel API Interaction ---
async def make_api_request(
    method: str, endpoint: str, api_type: str,
    interaction: Optional[discord.Interaction],
    json_data: Optional[dict] = None, params: Optional[dict] = None
) -> Optional[Union[dict, list, Any]]:
    if not bot.http_session or bot.http_session.closed:
        if bot.http_session and bot.http_session.closed:
            logger.warning("HTTP session was closed, attempting to recreate.")
            bot.http_session = aiohttp.ClientSession(headers={"User-Agent": "ConvoyDiscordBot/1.1"})
        else:
            logger.error("Bot HTTP session not initialized.")
            if interaction and not interaction.response.is_done(): 
                await send_error_embed(interaction, "Bot Error", "HTTP session not ready.")
            return None

    base_url = CONVOY_APP_API_URL if api_type == 'application' else CONVOY_CLIENT_API_URL
    api_key = CONVOY_APP_API_KEY if api_type == 'application' else CONVOY_CLIENT_API_KEY
    
    url = f"{base_url}/{endpoint.lstrip('/')}"
    headers = {
        'Authorization': f'Bearer {api_key}',
        'Content-Type': 'application/json',
        'Accept': 'application/json',
    }

    try:
        async with bot.http_session.request(method, url, headers=headers, json=json_data, params=params, timeout=aiohttp.ClientTimeout(total=45)) as response:
            response_text = await response.text()

            if 200 <= response.status < 300:
                if response.status == 204: 
                    return {"status_code": 204, "message": "Action completed."}
                try:
                    if 'application/json' in response.headers.get('Content-Type', ''):
                        return await response.json()
                    return {"raw_content": response_text, "status_code": response.status}
                except (json.JSONDecodeError, aiohttp.ContentTypeError) as json_err:
                    logger.error(f"Failed to decode JSON from {method} {url}. Status: {response.status}. Error: {json_err}")
                    if interaction and not interaction.response.is_done():
                        await send_error_embed(interaction, "API Response Error", f"Invalid server response for `{endpoint}` (Status: {response.status}).\n```\n{response_text[:1000]}\n```")
                    return None
            else:
                error_details = f"Endpoint: `{endpoint}` (API: {api_type})\nStatus: `{response.status}`"
                try:
                    error_json = json.loads(response_text)
                    errors = error_json.get('errors', [])
                    if errors and isinstance(errors, list):
                        msgs = [f"- {e.get('code', 'Error')}: {e.get('detail', 'Unknown')}" for e in errors]
                        error_details += "\n**Details:**\n" + "\n".join(msgs)
                    elif isinstance(error_json, dict) and 'message' in error_json:
                        error_details += f"\n**Message:** {error_json['message']}"
                    else:
                        error_details += f"\n```json\n{json.dumps(error_json, indent=2)[:1000]}\n```"
                except json.JSONDecodeError:
                    error_details += f"\n```\n{response_text[:1000]}\n```"
                
                logger.warning(f"Panel API Error: {error_details}")
                if interaction and not interaction.response.is_done():
                    await send_error_embed(interaction, "Panel API Error", error_details)
                return None

    except aiohttp.ClientConnectorError as e:
        logger.error(f"Network Error connecting to {base_url}: {e}")
        if interaction and not interaction.response.is_done(): 
            await send_error_embed(interaction, f"{Elookup('network')} Network Error", f"Could not connect to Panel API at `{CONVOY_API_URL_BASE}`.")
        return None
    except asyncio.TimeoutError:
        logger.error(f"Timeout Error for {method} {url}")
        if interaction and not interaction.response.is_done(): 
            await send_error_embed(interaction, f"{Elookup('loading')} Network Timeout", f"Request to Panel API timed out.")
        return None
    except Exception as e:
        logger.error(f"Unexpected API Request Error ({api_type} {endpoint}): {e}", exc_info=True)
        if interaction and not interaction.response.is_done(): 
            await send_error_embed(interaction, f"{Elookup('error')} Bot Error", "Unexpected error talking to the API.")
        return None

# --- Server Select Dropdown & Action View ---
class ServerSelectDropdown(discord.ui.Select):
    def __init__(self, servers: List[dict], placeholder: str, custom_id_prefix: str, server_formatter=None):
        self._servers_dict = {str(s.get('uuid') if s.get('uuid') else s.get('id')): s for s in servers if s.get('uuid') or s.get('id')}
        options = self._create_options(servers, server_formatter)
        super().__init__(
            placeholder=placeholder if options else "No servers available",
            min_values=1, max_values=1, options=options[:25],
            custom_id=f"{custom_id_prefix}:{random.randint(1000,9999)}", disabled=not options
        )

    def _create_options(self, server_list: List[Dict], server_formatter=None) -> List[discord.SelectOption]:
        options = []
        for i, server in enumerate(server_list):
            if i >= 100: break
            server_uuid = server.get('uuid') 
            server_id_app = server.get('id') 
            value_to_use = str(server_uuid if server_uuid else server_id_app)
            if not value_to_use: continue

            name = server.get('name', f'Server ID: {server_id_app or server_uuid}')[:90]
            
            if server_formatter:
                label, desc = server_formatter(server)
            else:
                status = server.get('status', '')
                node = server.get('node_id', '') 
                label = f"{name} ({server_id_app or server_uuid})"
                desc = f"Status: {status or 'N/A'} | Node: {node or 'N/A'}"
            
            options.append(discord.SelectOption(label=label[:100], value=value_to_use, description=desc[:100]))
        if not options:
            options.append(discord.SelectOption(label="No servers found.", value="no_servers_found", default=True))
        return options

    async def callback(self, interaction: discord.Interaction):
        pass 

class ServerActionView(discord.ui.View):
    def __init__(self, interaction: discord.Interaction, all_servers: List[dict], action_name: str, placeholder: str, custom_id_prefix: str, server_formatter=None):
        super().__init__(timeout=300)
        self.interaction = interaction
        self.action_name = action_name
        self.selected_server_uuid: Optional[str] = None 
        self.selected_server_data: Optional[dict] = None
        self.message: Optional[discord.WebhookMessage] = None

        self.dropdown = ServerSelectDropdown(all_servers, placeholder, custom_id_prefix, server_formatter)
        self.dropdown.callback = self.dropdown_callback
        self.add_item(self.dropdown)

        cancel_button = discord.ui.Button(label="Cancel", style=discord.ButtonStyle.red, custom_id="cancel_action")
        cancel_button.callback = self.cancel_callback
        self.add_item(cancel_button)

    async def dropdown_callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.interaction.user.id:
            await interaction.response.send_message(f"{Elookup('error')} This is NOT your menu!", ephemeral=True)
            return
        selection = interaction.data['values'][0]
        if selection == "no_servers_found":
            await interaction.response.defer()
            await interaction.edit_original_response(content="No server selected or available.", view=None, embed=None)
            self.stop()
            return
        self.selected_server_uuid = selection 
        self.selected_server_data = self.dropdown._servers_dict.get(self.selected_server_uuid)
        await interaction.response.defer()
        self.stop()

    async def cancel_callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.interaction.user.id:
            await interaction.response.send_message(f"{Elookup('error')} This is NOT your menu!", ephemeral=True)
            return
        await interaction.response.edit_message(content="Action cancelled.", view=None, embed=None)
        self.selected_server_uuid = None
        self.stop()

    async def start(self, message_content: str, embed: Optional[discord.Embed] = None):
        await self.interaction.followup.send(content=message_content, embed=embed, view=self, ephemeral=True)
        self.message = await self.interaction.original_response()

    async def on_timeout(self):
        if not self.message:
            try: self.message = await self.interaction.original_response()
            except discord.HTTPException: pass
        if self.message:
            try:
                for item in self.children: item.disabled = True
                await self.message.edit(content=f"{Elookup('loading')} Action timed out.", view=self, embed=None)
            except discord.HTTPException: pass

# --- Account Linking ---
def load_linked_accounts_sync() -> Dict[str, str]: 
    return load_json_file(LINKED_ACCOUNTS_FILE, {})

def save_linked_accounts_sync(accounts: Dict[str, str]):
    if not save_json_file(LINKED_ACCOUNTS_FILE, accounts):
        logger.critical(f"Failed to save {LINKED_ACCOUNTS_FILE}!")

async def get_linked_convoy_id(discord_id: int) -> Optional[str]:
    return bot.linked_accounts.get(str(discord_id))

async def link_user_account(discord_id: int, convoy_user_id: str):
    bot.linked_accounts[str(discord_id)] = str(convoy_user_id)
    save_linked_accounts_sync(bot.linked_accounts)

async def unlink_user_account(discord_id: int) -> bool:
    if str(discord_id) in bot.linked_accounts:
        del bot.linked_accounts[str(discord_id)]
        save_linked_accounts_sync(bot.linked_accounts)
        return True
    return False

# --- Invite Count Management ---
def save_invite_counts_sync(counts: Dict[str, Dict[str, int]]):
    if not save_json_file(INVITE_COUNTS_FILE, counts):
        logger.critical(f"Failed to save {INVITE_COUNTS_FILE}!")

def get_user_invite_count(guild_id: int, user_id: int) -> int:
    global invite_counts
    return invite_counts.get(str(guild_id), {}).get(str(user_id), 0)

def increment_invite_count(guild_id: int, user_id: int):
    global invite_counts
    gid_str, uid_str = str(guild_id), str(user_id)
    if gid_str not in invite_counts: 
        invite_counts[gid_str] = {}
    current_count = invite_counts[gid_str].get(uid_str, 0)
    invite_counts[gid_str][uid_str] = current_count + 1
    save_invite_counts_sync(invite_counts)
    logger.info(f"Incremented invite count for user {user_id} in guild {guild_id} to {current_count + 1}")

def reset_user_invites(guild_id: int, user_id: int) -> bool:
    global invite_counts
    gid_str, uid_str = str(guild_id), str(user_id)
    if gid_str in invite_counts and uid_str in invite_counts[gid_str]:
        original_count = invite_counts[gid_str][uid_str]
        invite_counts[gid_str][uid_str] = 0
        logger.info(f"Reset invite count for user {user_id} in guild {guild_id} from {original_count} to 0.")
        save_invite_counts_sync(invite_counts)
        return True
    return False

# --- Confirmation View ---
class ConfirmView(discord.ui.View):
    def __init__(self, authorized_user_id: int, timeout: int = 60, confirm_label="Confirm", cancel_label="Cancel", confirm_style=discord.ButtonStyle.danger):
        super().__init__(timeout=timeout)
        self.authorized_user_id = authorized_user_id
        self.confirmed: Optional[bool] = None
        self.message: Optional[discord.Message] = None

        self.confirm_btn = discord.ui.Button(label=confirm_label, style=confirm_style)
        self.confirm_btn.callback = self.confirm_button_callback
        self.add_item(self.confirm_btn)

        self.cancel_btn = discord.ui.Button(label=cancel_label, style=discord.ButtonStyle.secondary)
        self.cancel_btn.callback = self.cancel_button_callback
        self.add_item(self.cancel_btn)
        
    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.authorized_user_id:
            await interaction.response.send_message(f"{Elookup('error')} This confirmation is not for you!", ephemeral=True)
            return False
        return True

    async def _disable_all_buttons(self, interaction: Optional[discord.Interaction] = None):
        for item in self.children:
            if isinstance(item, discord.ui.Button): 
                item.disabled = True
        try:
            if interaction and not interaction.response.is_done():
                await interaction.response.edit_message(view=self)
            elif self.message:
                await self.message.edit(view=self)
        except discord.HTTPException: 
            pass

    async def confirm_button_callback(self, interaction: discord.Interaction):
        self.confirmed = True
        await self._disable_all_buttons(interaction)
        try:
            await interaction.edit_original_response(content=f"{Elookup('confirm')} Confirmed. Processing...", view=self)
        except discord.HTTPException:
            if self.message: 
                await self.message.edit(content=f"{Elookup('confirm')} Confirmed. Processing...", view=self)
        self.stop()

    async def cancel_button_callback(self, interaction: discord.Interaction):
        self.confirmed = False
        await self._disable_all_buttons(interaction)
        try:
            await interaction.edit_original_response(content=f"{Elookup('cancel')} Action cancelled.", view=self)
        except discord.HTTPException:
            if self.message: 
                await self.message.edit(content=f"{Elookup('cancel')} Action cancelled.", view=self)
        self.stop()

    async def on_timeout(self):
        if self.confirmed is None:
            await self._disable_all_buttons()
            if self.message:
                try: 
                    await self.message.edit(content=f"{Elookup('loading')} Confirmation timed out.", view=self)
                except discord.HTTPException: 
                    pass

# --- Admin Commands Group ---
admin_group = app_commands.Group(name="admin", description=f"{Elookup('admin')} Admin-only VPS commands")

# --- Server List View (Pagination) ---
class ServerListView(discord.ui.View):
    def __init__(self, interaction: discord.Interaction, initial_data: Dict[str, Any], items_per_page: int = 5, title_prefix="All Servers", api_type: str = 'application'):
        super().__init__(timeout=300)
        self.original_command_interaction = interaction
        self.items_per_page = items_per_page
        self.api_type = api_type 

        self.pagination_meta = initial_data.get('meta', {}).get('pagination', {})
        self.current_page = self.pagination_meta.get('current_page', 1)
        self.max_page = self.pagination_meta.get('total_pages', 1)
        self.total_items = self.pagination_meta.get('total', 0)
        self.servers = initial_data.get('data', [])
        
        self.view_message: Optional[discord.WebhookMessage] = None
        self.title_prefix = title_prefix 
        self.user_panel_id_filter = None 

        self._prev_button = discord.ui.Button(label="⬅️ Previous", style=discord.ButtonStyle.blurple, row=0)
        self._prev_button.callback = self._previous_button_callback 
        self.add_item(self._prev_button)

        self._page_indicator = discord.ui.Button(label="Page 1/1", style=discord.ButtonStyle.grey, disabled=True, row=0)
        self.add_item(self._page_indicator)

        self._next_button = discord.ui.Button(label="Next ➡️", style=discord.ButtonStyle.blurple, row=0)
        self._next_button.callback = self._next_button_callback
        self.add_item(self._next_button)
        
        self._update_button_states()

    async def _fetch_page_data(self, page_num: int, button_interaction: discord.Interaction) -> bool:
        params = {'page': page_num, 'per_page': self.items_per_page}
        if self.user_panel_id_filter and self.api_type == 'application': 
            params['filter[user_id]'] = self.user_panel_id_filter
        
        response_data = await make_api_request('GET', '/servers', self.api_type, button_interaction, params=params)

        if response_data and isinstance(response_data.get('data'), list):
            self.servers = response_data['data']
            self.pagination_meta = response_data.get('meta', {}).get('pagination', {})
            self.current_page = self.pagination_meta.get('current_page', page_num)
            new_max_page = self.pagination_meta.get('total_pages', 1)
            if new_max_page == 0 and self.total_items > 0:
                self.max_page = math.ceil(self.total_items / self.items_per_page) if self.total_items > 0 else 1
            elif new_max_page > 0: 
                self.max_page = new_max_page
            else: 
                self.max_page = 1
            self.total_items = self.pagination_meta.get('total', 0)
            return True
        return False

    def _create_page_embed(self) -> discord.Embed:
        embed = discord.Embed(
            title=f"{Elookup('server', '🖥️')} {self.title_prefix} (Page {self.current_page}/{self.max_page})",
            color=discord.Color.purple(), timestamp=datetime.now(timezone.utc)
        )
        if not self.servers:
            embed.description = "🍃 No servers found on this page."
        else:
            description_parts = []
            for server_obj in self.servers:
                server_data = server_obj if self.api_type == 'application' else server_obj.get('attributes', {})
                
                name = server_data.get('name', '`N/A`')
                s_id_app = server_data.get('id', '`N/A`') 
                s_uuid = server_data.get('uuid', '`N/A`') 
                display_id = s_uuid if s_uuid != '`N/A`' else s_id_app

                status = server_data.get('status') 
                if self.api_type == 'client': 
                    status = server_data.get('state', server_data.get('current_state', status)) 

                status_str = f"`{str(status).capitalize() if status else 'Unknown'}`"
                
                limits = server_data.get('limits', {})
                cpu_cores = limits.get('cpu', 'N/A')
                memory_mb = limits.get('memory') 
                memory_gb_str = f"{memory_mb / 1024:.1f} GB" if isinstance(memory_mb, (int, float)) and memory_mb > 0 else f"{memory_mb} MB" if memory_mb else "N/A"
                
                node_id_val = server_data.get('node_id', server_data.get('node', 'N/A')) 
                owner_id = server_data.get('user_id', server_data.get('user', 'N/A')) 
                owner_info = f"Owner: `{owner_id}`" if owner_id != 'N/A' else ""
                
                description_parts.append(
                    f"🔹 **{name}** (ID: `{display_id}`)\n"
                    f"   Status: {status_str} | Node: `{node_id_val}`\n"
                    f"   CPU: `{cpu_cores}` Cores/100% | RAM: `{memory_gb_str}` | {owner_info}"
                )
            embed.description = "\n---\n".join(description_parts)

        footer_text = f"Total Servers: {self.total_items}"
        if self.title_prefix == "All Panel Servers":
            footer_text += " | Use /admin assign to change owner."
        embed.set_footer(text=footer_text)
        return embed

    def _update_button_states(self):
        self._prev_button.disabled = self.current_page <= 1
        self._next_button.disabled = self.current_page >= self.max_page
        self._page_indicator.label = f"Page {self.current_page}/{self.max_page}"
        if self.total_items == 0:
            self._prev_button.disabled = True
            self._next_button.disabled = True

    async def _update_view_message(self, content: Optional[str] = None, embed: Optional[discord.Embed] = None, view: Optional[discord.ui.View] = None):
        if not self.view_message: 
            return
        try:
            await self.view_message.edit(content=content, embed=embed, view=view if view is not None else self)
        except discord.HTTPException as e:
            logger.error(f"ServerListView: Failed to edit view_message (ID: {self.view_message.id}): {e}")

    async def _previous_button_callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.original_command_interaction.user.id:
            return await interaction.response.send_message(f"{Elookup('error')} This isn't your menu!", ephemeral=True)
        await interaction.response.defer()
        if self.current_page > 1:
            await self._update_view_message(content=f"{Elookup('loading')} Fetching previous page...", embed=None, view=None)
            if await self._fetch_page_data(self.current_page - 1, interaction): 
                self._update_button_states()
                await self._update_view_message(embed=self._create_page_embed())
            else:
                self._update_button_states()
                await self._update_view_message(content=None, embed=self._create_page_embed())

    async def _next_button_callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.original_command_interaction.user.id:
            return await interaction.response.send_message(f"{Elookup('error')} This isn't your menu!", ephemeral=True)
        await interaction.response.defer()
        if self.current_page < self.max_page:
            await self._update_view_message(content=f"{Elookup('loading')} Fetching next page...", embed=None, view=None)
            if await self._fetch_page_data(self.current_page + 1, interaction):
                self._update_button_states()
                await self._update_view_message(embed=self._create_page_embed())
            else:
                self._update_button_states()
                await self._update_view_message(content=None, embed=self._create_page_embed())

    async def start(self):
        self._update_button_states()
        embed = self._create_page_embed()
        try:
            await self.original_command_interaction.followup.send(embed=embed, view=self, ephemeral=False) 
            self.view_message = await self.original_command_interaction.original_response()
        except discord.HTTPException as e:
            logger.error(f"ServerListView: Failed to send initial '{self.title_prefix}' list: {e}")
            try: 
                await self.original_command_interaction.followup.send(f"{Elookup('error')} Error displaying {self.title_prefix.lower()} list.", ephemeral=True)
            except: 
                pass

    async def on_timeout(self):
        if not self.view_message:
            try: 
                self.view_message = await self.original_command_interaction.original_response()
            except discord.HTTPException: 
                return
        
        for item in self.children:
            if isinstance(item, discord.ui.Button): 
                item.disabled = True
        
        final_embed = self._create_page_embed()
        final_embed.set_footer(text=f"Total Servers: {self.total_items} | View timed out {Elookup('loading')}")
        await self._update_view_message(embed=final_embed)

@admin_group.command(name="serverlist", description=f"{Elookup('server', '📄')} List all servers on the panel (paginated).")
@is_vps_creator()
async def admin_list_all_servers(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=False) 
    params = {'page': 1, 'per_page': 5}
    response_data = await make_api_request('GET', '/servers', 'application', interaction, params=params)

    if response_data and isinstance(response_data.get('data'), list):
        if not response_data['data'] and response_data.get('meta', {}).get('pagination', {}).get('total', 0) == 0:
            await send_info_embed(interaction, "List All Servers", "🍃 No servers found on the panel.", ephemeral=False)
            return
        view = ServerListView(interaction, response_data, items_per_page=5, title_prefix="All Panel Servers", api_type='application')
        await view.start() 
    elif response_data is not None:
        if not interaction.response.is_done():
            await send_error_embed(interaction, "API Format Error", f"Received unexpected data format from the API: ```{str(response_data)[:1000]}```", ephemeral=True)

# --- Admin Create Server Modal & Command ---
class AdminCreateServerModal(ui.Modal, title='✨ Create New VPS (Admin)'):
    server_name = ui.TextInput(label='Server Name', placeholder='My Awesome Server', required=True, max_length=100)
    hostname_prefix = ui.TextInput(label='Hostname Prefix (Optional)', placeholder='my-server', required=False, max_length=60)
    cpu_cores = ui.TextInput(label='CPU Cores', placeholder='e.g., 4', required=True, max_length=4)
    memory_mb = ui.TextInput(label='Memory (RAM) in MB', placeholder='e.g., 4096', required=True, max_length=6)
    disk_mb = ui.TextInput(label='Disk Size in MB', placeholder='e.g., 20480', required=True, max_length=7)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            self.cpu_val = int(self.cpu_cores.value)
            self.memory_val = int(self.memory_mb.value)
            self.disk_val = int(self.disk_mb.value)
            if self.cpu_val <= 0 or self.memory_val <= 0 or self.disk_val <= 0: 
                raise ValueError("Resources must be positive.")
        except ValueError:
            await interaction.response.send_message(f"{Elookup('error')} Invalid resource value. Enter positive numbers.", ephemeral=True)
            self.stop()
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        self.modal_interaction = interaction
        self.stop()

    async def on_error(self, interaction: discord.Interaction, error: Exception):
        logger.error(f"Error in AdminCreateServerModal: {error}", exc_info=True)
        if not interaction.response.is_done():
            try: 
                await interaction.response.send_message(f"{Elookup('error')} Oops! Form error. Please try again.", ephemeral=True)
            except discord.HTTPException: 
                pass
        else:
            try: 
                await interaction.followup.send(f"{Elookup('error')} Oops! Form error. Please try again.", ephemeral=True)
            except discord.HTTPException: 
                pass
        self.stop()

@admin_group.command(name="create", description=f"{Elookup('server', '🛠️')} Guided VPS creation (Admin).")
@is_vps_creator()
@app_commands.describe(assign_to="Optional: Assign to this Discord user.", node_id="Optional: Node ID.", template_uuid="Optional: Template UUID.")
async def admin_create_vps(interaction: discord.Interaction, assign_to: Optional[discord.User] = None, node_id: Optional[int] = None, template_uuid: Optional[str] = None):
    original_cmd_interaction = interaction
    target_user_id_panel: Optional[int] = None
    dm_user = original_cmd_interaction.user

    if assign_to:
        dm_user = assign_to
        linked_id = await get_linked_convoy_id(assign_to.id)
        if not linked_id:
            await send_error_embed(original_cmd_interaction, "User Not Linked", f"{assign_to.mention} needs to `/link` their account first before you can assign a server to them.")
            return
        target_user_id_panel = int(linked_id)
    else:
        self_linked_id = await get_linked_convoy_id(original_cmd_interaction.user.id)
        if not self_linked_id:
            await send_error_embed(original_cmd_interaction, "Admin Not Linked", "You (admin) need to `/link` your own account to the panel before creating servers for yourself.")
            return
        target_user_id_panel = int(self_linked_id)

    selected_node_id = node_id
    if not selected_node_id:
        if DEFAULT_NODE_ID:
            selected_node_id = DEFAULT_NODE_ID
            logger.info(f"Using default Node ID: {selected_node_id}")
        else:
            await send_error_embed(original_cmd_interaction, "Node Error", "No Node ID specified and no default node is configured. Please specify a `node_id` or set a default.")
            return

    selected_template_uuid = template_uuid
    if not selected_template_uuid:
        if DEFAULT_TEMPLATE_UUID:
            selected_template_uuid = DEFAULT_TEMPLATE_UUID
            logger.info(f"Using default Template UUID: {selected_template_uuid}")
        else:
            await send_error_embed(original_cmd_interaction, "Template Error", "No Template UUID specified and no default template is configured. Please specify a `template_uuid` or set a default.")
            return
    
    modal = AdminCreateServerModal()
    await original_cmd_interaction.response.send_modal(modal)
    await modal.wait()

    if not hasattr(modal, 'modal_interaction') or modal.modal_interaction is None:
        logger.info("Admin VPS creation modal was not submitted or timed out.")
        return

    modal_interaction = modal.modal_interaction
    
    temp_password = generate_compliant_password(length=16)
    
    hostname_prefix_val = modal.hostname_prefix.value.strip().lower().replace(' ', '-') if modal.hostname_prefix.value else modal.server_name.value.strip().lower().replace(' ', '-')
    hostname_prefix_val = "".join(c for c in hostname_prefix_val if c.isalnum() or c == '-')[:60]
    full_hostname = f"{hostname_prefix_val}.{DEFAULT_SERVER_HOSTNAME_SUFFIX}"

    assigned_ip_from_file = get_and_remove_first_ip() 
    next_vmid = random.randint(200, 9999)
    logger.info(f"Generated VMID for admin VPS creation: {next_vmid}")

    # Fixed payload for Convoy API
    payload = {
        "name": modal.server_name.value.strip(),
        "user_id": target_user_id_panel,
        "node_id": selected_node_id,
        "vmid": next_vmid,
        "cores": modal.cpu_val,
        "memory": modal.memory_val,
        "disk": modal.disk_val,
        "datastore": "local",
        "template": selected_template_uuid,
        "storage_type": "qcow2",
        "network_type": "bridge",
        "bandwidth_limit": 0,
        "ip_addresses": [assigned_ip_from_file] if assigned_ip_from_file else [],
        "start_on_creation": True
    }

    logger.info(f"Admin VPS creation payload: {json.dumps(payload, indent=2)}")
    
    creation_response = await make_api_request('POST', '/servers', 'application', modal_interaction, json_data=payload)

    if creation_response and isinstance(creation_response, dict):
        server_name_resp = creation_response.get('name', modal.server_name.value)
        server_id = creation_response.get('id', 'N/A')
        
        details_embed = discord.Embed(
            title=f"{Elookup('success')} Admin VPS Created: {server_name_resp}",
            description=f"A new VPS has been successfully provisioned for {dm_user.mention}!", 
            color=discord.Color.brand_green()
        )
        details_embed.add_field(name="🔗 Panel Link", value=f"<{PANEL_BASE_URL}/server/{server_id}>", inline=False)
        details_embed.add_field(name="🏷️ Server Name", value=f"`{server_name_resp}`", inline=True)
        details_embed.add_field(name="🆔 Server ID", value=f"`{server_id}`", inline=True)
        details_embed.add_field(name="🌐 Hostname", value=f"`{full_hostname}`", inline=True)
        
        if assigned_ip_from_file:
            details_embed.add_field(name="🔌 Primary IP", value=f"`{assigned_ip_from_file}`", inline=True)
        
        details_embed.add_field(name=f"{Elookup('cpu')} CPU Cores", value=f"`{modal.cpu_val}`", inline=True)
        details_embed.add_field(name=f"{Elookup('ram')} Memory", value=f"`{modal.memory_val} MB`", inline=True)
        details_embed.add_field(name=f"{Elookup('disk')} Disk", value=f"`{modal.disk_val} MB`", inline=True)
        details_embed.add_field(name=f"{Elookup('password')} Initial Root Password", value=f"||`{temp_password}`|| (Login and change immediately!)", inline=False)
        details_embed.set_footer(text=f"VMID: {next_vmid} | Node: {selected_node_id} | Template: {selected_template_uuid[:10]}...")

        dm_sent_successfully = False
        try:
            dm_channel = await dm_user.create_dm()
            await dm_channel.send(embed=details_embed)
            dm_sent_successfully = True
        except discord.Forbidden:
            await modal_interaction.followup.send(
                content=f"{Elookup('success')} VPS **{server_name_resp}** created for {dm_user.mention}, but **could not send DM with details.** Details below:",
                embed=details_embed, ephemeral=False
            )
        except Exception as e:
            logger.error(f"Error sending DM for admin-created VPS: {e}")
            await modal_interaction.followup.send(
                content=f"{Elookup('error')} VPS **{server_name_resp}** created for {dm_user.mention}, but **error sending DM.** Details below:",
                embed=details_embed, ephemeral=False
            )

        if dm_sent_successfully:
            await send_success_embed(modal_interaction, "VPS Created!", f"Successfully started VPS **{server_name_resp}** (ID: `{server_id}`). Details have been DM'd to {dm_user.mention}.", ephemeral=True)
        
        # Log the creation
        await send_vps_log("Admin Creation", original_cmd_interaction.user, details_embed, server_id, server_id, server_name_resp)

# --- User VPS Creation System ---
class PlanSelectView(discord.ui.View):
    def __init__(self, user_id: int, interaction: discord.Interaction):
        super().__init__(timeout=180)
        self.user_id = user_id
        self.interaction_context = interaction 
        self.selected_plan_type: Optional[str] = None 
        self.selected_plan_data: Optional[Dict] = None 
        self.message: Optional[discord.WebhookMessage] = None
        self._add_buttons_and_selects()

    def _add_buttons_and_selects(self):
        boost_options = []
        if BOOST_REWARDS_ENABLED and BOOST_REWARD_TIERS:
            for i, tier in enumerate(BOOST_REWARD_TIERS):
                boost_options.append(discord.SelectOption(
                    label=f"{Elookup('boost_plan','🚀')} {tier.get('name', f'Boost Tier {i+1}')} ({tier.get('server_boosts_required','N/A')} Boosts)",
                    value=f"boost_{i}",
                    description=f"{tier.get('ram_gb','N/A')}GB RAM, {tier.get('cpu_cores','N/A')} CPU, {tier.get('disk_gb','N/A')}GB Disk"[:100]
                ))
        if boost_options:
            self.add_item(discord.ui.Select(placeholder=f"{Elookup('boost_plan','🚀')} Select Boost Plan", options=boost_options[:25], custom_id="select_boost_plan"))
        else: 
            self.add_item(discord.ui.Select(placeholder=f"{Elookup('boost_plan','🚀')} Boost Rewards Unavailable", options=[discord.SelectOption(label="N/A", value="na_boost")], disabled=True, custom_id="select_boost_plan_disabled"))

        invite_options = []
        if INVITE_REWARDS_ENABLED_GLOBAL and INVITE_REWARD_TIERS:
            for i, tier in enumerate(INVITE_REWARD_TIERS):
                invite_options.append(discord.SelectOption(
                    label=f"{Elookup('invite_plan','💌')} {tier.get('name', f'Invite Tier {i+1}')} ({tier.get('invites_required','N/A')} Invites)",
                    value=f"invite_{i}",
                    description=f"{tier.get('ram_gb','N/A')}GB RAM, {tier.get('cpu_cores','N/A')} CPU, {tier.get('disk_gb','N/A')}GB Disk"[:100]
                ))
        if invite_options:
            self.add_item(discord.ui.Select(placeholder=f"{Elookup('invite_plan','💌')} Select Invite Plan", options=invite_options[:25], custom_id="select_invite_plan"))
        else:
            self.add_item(discord.ui.Select(placeholder=f"{Elookup('invite_plan','💌')} Invite Rewards Unavailable", options=[discord.SelectOption(label="N/A", value="na_invite")], disabled=True, custom_id="select_invite_plan_disabled"))

        if PAID_PLANS_DATA:
            self.add_item(discord.ui.Button(label=f"{Elookup('paid_plan','💰')} Paid Plan Request", style=discord.ButtonStyle.success, custom_id="select_paid_plan"))

        for child in self.children:
            if isinstance(child, discord.ui.Select):
                child.callback = self.select_callback
            elif isinstance(child, discord.ui.Button):
                child.callback = self.button_callback
    
    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message(f"{Elookup('error')} Not your menu!", ephemeral=True)
            return False
        return True

    async def select_callback(self, interaction: discord.Interaction):
        selected_value = interaction.data['values'][0]
        if "_" not in selected_value or selected_value.startswith("na_"):
            await interaction.response.defer() 
            return

        plan_category, plan_index_str = selected_value.split('_', 1)
        
        try:
            plan_index = int(plan_index_str)
        except ValueError:
            await interaction.response.send_message(f"{Elookup('error')} Invalid plan selection format.", ephemeral=True)
            self.stop()
            return

        self.selected_plan_type = plan_category

        if plan_category == "boost" and 0 <= plan_index < len(BOOST_REWARD_TIERS):
            self.selected_plan_data = BOOST_REWARD_TIERS[plan_index]
        elif plan_category == "invite" and 0 <= plan_index < len(INVITE_REWARD_TIERS):
            self.selected_plan_data = INVITE_REWARD_TIERS[plan_index]
        else: 
            await interaction.response.send_message(f"{Elookup('error')} Invalid plan data. Contact admin.", ephemeral=True)
            self.stop()
            return

        await interaction.response.defer()
        if self.message:
            try:
                selected_label = self.selected_plan_data.get('name', f"{plan_category.title()} Plan")
                await self.message.edit(content=f"{Elookup('confirm','✅')} Plan selected: **{selected_label}**. Please wait, processing...", view=None, embed=None)
            except discord.HTTPException as e:
                 logger.error(f"PlanSelectView: Error editing message on selection: {e}")
        self.stop()

    async def button_callback(self, interaction: discord.Interaction):
        self.selected_plan_type = "paid"
        self.selected_plan_data = {"name": "Paid Plan Request"} 
        await interaction.response.defer()
        if self.message:
            try: 
                await self.message.edit(content=f"{Elookup('confirm','✅')} Selection: **Paid Plan Request**. Proceeding...", view=None, embed=None)
            except discord.HTTPException as e:
                logger.error(f"PlanSelectView: Error editing message on button press: {e}")
        self.stop()
    
    async def on_timeout(self):
        if self.message and self.selected_plan_type is None:
            try:
                for item in self.children: 
                    item.disabled = True
                await self.message.edit(content=f"{Elookup('loading')} Plan selection timed out. Use `/create` again if you wish to proceed.", view=self, embed=None)
            except discord.HTTPException as e:
                logger.warning(f"PlanSelectView: Error editing message on timeout: {e}")

class AdminConfirmationView(discord.ui.View): 
    def __init__(self, requesting_user_id: int, plan_name: str, server_payload: dict, temp_password: str, assigned_ip_ref: str, timeout=7200):
        super().__init__(timeout=timeout)
        self.requesting_user_id = requesting_user_id
        self.plan_name = plan_name
        self.server_payload = server_payload 
        self.temp_password = temp_password
        self.assigned_ip_ref = assigned_ip_ref 
        self.status: Optional[bool] = None
        self.response_admin: Optional[discord.User] = None
        self.message: Optional[discord.Message] = None
        
    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if not interaction.guild or not isinstance(interaction.user, discord.Member) or not check_is_vps_creator(interaction):
            await interaction.response.send_message(f"{Elookup('error')} Only VPS Creators can approve/deny these requests.", ephemeral=True)
            return False
        return True

    async def _update_view_on_action(self, interaction: discord.Interaction, approved: bool):
        self.status = approved
        self.response_admin = interaction.user
        
        for item in self.children:
            if isinstance(item, discord.ui.Button): 
                item.disabled = True

        if self.message and self.message.embeds:
            embed = self.message.embeds[0].copy()
            action_taken_str = "Approved" if approved else "Denied"
            action_color = discord.Color.green() if approved else discord.Color.red()
            
            embed.title = f"{Elookup('info')} VPS Request {action_taken_str}"
            embed.color = action_color
            embed.add_field(name=f"{action_taken_str} By", value=f"{interaction.user.mention} at {discord.utils.format_dt(datetime.now(timezone.utc))}", inline=False)
            
            await interaction.response.edit_message(embed=embed, view=self)
        else:
            await interaction.response.edit_message(content=f"Request {action_taken_str.lower()} by {interaction.user.mention}. Buttons disabled.", view=self)
        self.stop()

    @discord.ui.button(label="Approve Request", style=discord.ButtonStyle.green, custom_id="vps_req_approve_btn")
    async def approve_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._update_view_on_action(interaction, True)
    
    @discord.ui.button(label="Deny Request", style=discord.ButtonStyle.red, custom_id="vps_req_deny_btn")
    async def deny_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._update_view_on_action(interaction, False)
    
    async def on_timeout(self):
        if self.status is None and self.message:
            embed_title = f"{Elookup('loading')} VPS Request Timed Out"
            embed_desc = "This request expired due to no administrator response within the allocated time."
            
            new_embed: Optional[discord.Embed] = None
            if self.message.embeds:
                embed = self.message.embeds[0].copy()
                embed.title = embed_title
                embed.color = discord.Color.dark_gray()
                embed.add_field(name="Status Update", value=embed_desc, inline=False)
                new_embed = embed
            else:
                new_embed = discord.Embed(title=embed_title, description=embed_desc, color=discord.Color.dark_gray())

            for item in self.children: 
                if isinstance(item, discord.ui.Button): 
                    item.disabled = True
            try: 
                await self.message.edit(embed=new_embed, view=self)
            except discord.HTTPException as e: 
                logger.error(f"Error updating admin confirmation view on timeout: {e}")

@bot.tree.command(name="create", description=f"{Elookup('server')} Create a VPS based on available plans.")
async def create_vps(interaction: discord.Interaction):
    if interaction.guild and isinstance(interaction.user, discord.Member) and check_is_vps_creator(interaction):
        await send_error_embed(interaction, "Admin Command Available", f"VPS Creators should use `/admin create` for more control.")
        return

    await interaction.response.defer(ephemeral=True)
    user_panel_id = await get_linked_convoy_id(interaction.user.id)
    if not user_panel_id:
        await send_error_embed(interaction, "Account Not Linked", "Please use `/link` to link your Discord account to your panel account before creating a VPS.")
        return

    view = PlanSelectView(interaction.user.id, interaction)
    msg_obj = await send_info_embed(
        interaction, "VPS Creation: Select Your Plan", "Choose your desired VPS plan from the options below:", view=view, ephemeral=True
    )
    if not msg_obj:
        logger.error(f"Failed to get message object for PlanSelectView for user {interaction.user.id}.")
        await interaction.followup.send(f"{Elookup('error')} An error occurred trying to display plan selection. Please try again.", ephemeral=True)
        return
    view.message = msg_obj
    await view.wait()

    if view.selected_plan_type is None or view.selected_plan_data is None:
        if not view.is_finished() and view.selected_plan_type is None:
             await interaction.followup.send(f"{Elookup('info')} Plan selection was cancelled or no valid plan was chosen.", ephemeral=True)
        return

    plan_type = view.selected_plan_type
    plan_data = view.selected_plan_data 

    if plan_type == "paid":
        owner = await bot.fetch_user(BOT_OWNER_USER_ID) if BOT_OWNER_USER_ID else None
        owner_mention = owner.mention if owner else f"the Bot Owner (ID: {BOT_OWNER_USER_ID})"
        await send_info_embed(interaction, f"{Elookup('paid_plan','💰')} Paid Plan Request Initiated",
                              f"Thank you for your interest! Your request for a paid VPS has been noted.\n"
                              f"Please create a support ticket in our Discord server to discuss the details and payment. {owner_mention} or a staff member will assist you shortly.",
                              ephemeral=True)
        if owner:
            try: 
                await owner.send(f"🔔 User {interaction.user.mention} ({interaction.user.id}) has initiated a **paid VPS plan request** through the bot.")
            except discord.HTTPException as e: 
                logger.warning(f"Could not DM Bot Owner about paid plan request: {e}")
        return

    # Verify plan requirements
    if not all(k in plan_data for k in ['cpu_cores', 'ram_gb', 'disk_gb']):
        await send_error_embed(interaction, "Plan Configuration Error", "The selected plan is missing resource details. Please contact an admin.")
        return
        
    target_node_id = plan_data.get('node_id', DEFAULT_NODE_ID)
    selected_template_uuid = plan_data.get('template_uuid', DEFAULT_TEMPLATE_UUID)
    if not target_node_id or not selected_template_uuid:
        await send_error_embed(interaction, "Configuration Error", "The default node ID or template UUID for this reward plan is misconfigured. Please contact an admin.")
        return

    assigned_ip_from_file = get_and_remove_first_ip()
    if not assigned_ip_from_file:
        await send_error_embed(interaction, "Resource Error", "We've temporarily run out of available IP addresses for new servers. Please try again later or contact an administrator.")
        return
    
    temp_password = generate_compliant_password(length=12)
    plan_name_log = plan_data.get('name', f"{plan_type.title()} Reward Plan")

    # Verify Boost requirements
    if plan_type == "boost":
        if not BOOST_REWARDS_ENABLED:
            await send_error_embed(interaction, "Boost Rewards Disabled", "Server Boost rewards are currently disabled by the administrators.")
            return
        if not interaction.guild: 
            await send_error_embed(interaction, "Context Error", "Cannot verify boost status outside a server environment.")
            return
        
        member = interaction.guild.get_member(interaction.user.id)
        if not member:
            try: 
                member = await interaction.guild.fetch_member(interaction.user.id)
            except discord.NotFound:
                 await send_error_embed(interaction, "Verification Error", "Could not find your member information in this server.")
                 return
        
        boost_req = plan_data.get('server_boosts_required', 999)
        if not member.premium_since:
            await send_error_embed(interaction, "Boost Verification Failed", f"You are not currently boosting **{interaction.guild.name}**. This plan requires you to be an active booster of this server.")
            return

    # Verify Invite requirements
    elif plan_type == "invite":
        if not INVITE_REWARDS_ENABLED_GLOBAL:
            await send_error_embed(interaction, "Invite Rewards Disabled", "Invite-based rewards are currently disabled by the administrators.")
            return
        if not interaction.guild:
            await send_error_embed(interaction, "Context Error", "Cannot verify invite counts outside a server environment.")
            return

        invite_req = plan_data.get('invites_required', 999)
        verified_invites = get_user_invite_count(interaction.guild.id, interaction.user.id)
        if verified_invites < invite_req:
            await send_error_embed(interaction, "Invite Verification Failed", f"You currently have **{verified_invites}** tracked invites for this server. This plan requires **{invite_req}** invites. Keep inviting!")
            return

    # Create server payload for admin approval
    safe_user_name = "".join(filter(str.isalnum, interaction.user.name))[:10] or "user"
    server_name_gen = f"{plan_type[:3].upper()}-{safe_user_name}-{random.randint(100,999)}"[:50]
    hostname_gen = f"{server_name_gen.lower().replace(' ', '-')}.{DEFAULT_SERVER_HOSTNAME_SUFFIX}"
    
    next_vmid = random.randint(200, 9999) 
    logger.info(f"Generated VMID for user plan creation ({plan_name_log}): {next_vmid}")
    
    # Fixed payload for Convoy API
    server_creation_payload = {
        "name": server_name_gen,
        "user_id": int(user_panel_id),
        "node_id": target_node_id,
        "vmid": next_vmid,
        "cores": plan_data['cpu_cores'],
        "memory": plan_data['ram_gb'] * 1024,  # Convert GB to MB
        "disk": plan_data['disk_gb'] * 1024,   # Convert GB to MB
        "datastore": "local",
        "template": selected_template_uuid,
        "storage_type": "qcow2",
        "network_type": "bridge",
        "bandwidth_limit": 0,
        "ip_addresses": [assigned_ip_from_file],
        "start_on_creation": True
    }

    logger.info(f"IP '{assigned_ip_from_file}' from {IP_FILENAME} has been reserved for user {interaction.user.id}'s potential VPS ({plan_name_log}).")

    await send_info_embed(interaction, f"{Elookup('loading')} Request Submitted for Approval",
                          f"Your request for the **{plan_name_log}** VPS is now awaiting administrator approval.\n"
                          f"You will receive a DM once it has been reviewed. The IP address ` {assigned_ip_from_file} ` has been tentatively reserved for you if this request is approved.",
                          ephemeral=True)

    if not ADMIN_VPS_APPROVAL_CHANNEL_ID:
        logger.error("`admin_vps_approval_channel_id` is not set in config.json. Cannot send VPS creation request for admin approval.")
        await interaction.followup.send(f"{Elookup('error')} An internal configuration error occurred (admin approval channel not set). Please notify an administrator.", ephemeral=True)
        try:
            with open(IP_FILENAME, 'a') as f: 
                f.write(f"\n{assigned_ip_from_file}\n")
            logger.info(f"Returned IP '{assigned_ip_from_file}' to {IP_FILENAME} due to approval channel error.")
        except IOError: 
            logger.error(f"Failed to return IP '{assigned_ip_from_file}' to {IP_FILENAME}.")
        return
        
    admin_channel = bot.get_channel(ADMIN_VPS_APPROVAL_CHANNEL_ID)
    if not admin_channel or not isinstance(admin_channel, discord.TextChannel):
        logger.error(f"Admin approval channel ID {ADMIN_VPS_APPROVAL_CHANNEL_ID} is invalid or bot cannot access it.")
        await interaction.followup.send(f"{Elookup('error')} An internal error occurred (admin approval channel invalid). Please notify an administrator.", ephemeral=True)
        try:
            with open(IP_FILENAME, 'a') as f: 
                f.write(f"\n{assigned_ip_from_file}\n")
            logger.info(f"Returned IP '{assigned_ip_from_file}' to {IP_FILENAME} due to invalid approval channel.")
        except IOError: 
            logger.error(f"Failed to return IP '{assigned_ip_from_file}' to {IP_FILENAME}.")
        return

    approval_embed = discord.Embed(
        title=f"{Elookup('info')} New VPS Creation Request (User)",
        description=f"User {interaction.user.mention} (`{interaction.user.id}`) has requested a VPS through a reward plan:",
        color=discord.Color.orange(),
        timestamp=datetime.now(timezone.utc)
    )
    approval_embed.add_field(name="📜 Plan Requested", value=plan_name_log, inline=True)
    approval_embed.add_field(name="👤 Panel User ID", value=f"`{user_panel_id}`", inline=True)
    approval_embed.add_field(name="🖥️ Proposed Server Name", value=f"`{server_name_gen}`", inline=False)
    approval_embed.add_field(name="🌐 Proposed Hostname", value=f"`{hostname_gen}`", inline=False)
    approval_embed.add_field(name="🔌 Reserved IP", value=f"`{assigned_ip_from_file}`", inline=False)
    
    approval_embed.add_field(name="⚙️ Plan Specifications", 
                              value=f"{Elookup('cpu','⚙️')} CPU Cores: **{plan_data['cpu_cores']}**\n"
                                    f"{Elookup('ram','💾')} RAM: **{plan_data['ram_gb']}GB**\n"
                                    f"{Elookup('disk','📀')} Disk: **{plan_data['disk_gb']}GB SSD**", 
                              inline=False)
    approval_embed.add_field(name=f"{Elookup('password')} Temporary Root Password", value=f"||`{temp_password}`|| (User will be DMed this)", inline=False)
    approval_embed.set_footer(text=f"Target Node: {target_node_id} | Template: {selected_template_uuid[:12]}... | VMID: {next_vmid}")

    admin_conf_view = AdminConfirmationView(interaction.user.id, plan_name_log, server_creation_payload, temp_password, assigned_ip_from_file)
    
    try:
        ping_role = interaction.guild.get_role(VPS_CREATOR_ROLE_ID) if interaction.guild else None
        content_msg = f"{ping_role.mention if ping_role else '@VPS Creators'} New VPS creation request requires your approval:"
        
        approval_msg_obj = await admin_channel.send(content=content_msg, embed=approval_embed, view=admin_conf_view)
        admin_conf_view.message = approval_msg_obj
    except discord.HTTPException as e:
        logger.error(f"Failed to send VPS approval request to admin channel: {e}")
        await interaction.followup.send(f"{Elookup('error')} Failed to submit your request to the administrators due to a Discord error. Please try again later or contact support.", ephemeral=True)
        try:
            with open(IP_FILENAME, 'a') as f: 
                f.write(f"\n{assigned_ip_from_file}\n")
            logger.info(f"Returned IP '{assigned_ip_from_file}' to {IP_FILENAME} due to admin channel send error.")
        except IOError: 
            logger.error(f"Failed to return IP '{assigned_ip_from_file}' to {IP_FILENAME}.")
        return

    await admin_conf_view.wait()

    # Process admin decision
    if admin_conf_view.status is True:  # Approved
        creation_response = await make_api_request('POST', '/servers', 'application', interaction, json_data=server_creation_payload)

        if creation_response and isinstance(creation_response, dict):
            server_name_resp = creation_response.get('name', server_name_gen)
            server_id = creation_response.get('id', 'N/A')
            
            if plan_type == "invite" and interaction.guild:
                if reset_user_invites(interaction.guild.id, interaction.user.id):
                    logger.info(f"Reset invites for user {interaction.user.id} in guild {interaction.guild.id} after VPS creation.")
                else:
                    logger.warning(f"Failed to reset invites for user {interaction.user.id} after VPS creation.")
            
            details_desc = (
                f"🎉 Congratulations! Your **{plan_name_log}** VPS request was approved by {admin_conf_view.response_admin.mention} and your server is now ready!\n\n"
                f"**🔗 Panel Link:** <{PANEL_BASE_URL}/server/{server_id}>\n"
                f"**🏷️ Server Name:** `{server_name_resp}` (ID: `{server_id}`)\n"
                f"**🔌 Assigned IP Address:** `{assigned_ip_from_file}`\n"
                f"**🌐 Hostname:** `{hostname_gen}`\n\n"
                f"⚙️ **Server Resources:**\n"
                f"   {Elookup('cpu','⚙️')} CPU Cores: **{plan_data['cpu_cores']}**\n"
                f"   {Elookup('ram','💾')} RAM: **{plan_data['ram_gb']}GB**\n"
                f"   {Elookup('disk','📀')} Disk: **{plan_data['disk_gb']}GB SSD**\n\n"
                f"🔑 **Initial Root Password:** ||`{temp_password}`||\n\n"
                f"⚠️ **IMPORTANT:** Please log into your server via the panel or SSH as soon as possible and **change this temporary password.**"
            )
            dm_embed = discord.Embed(title=f"{Elookup('success')} Your RN Nodes VPS is Ready!", description=details_desc, color=discord.Color.brand_green())
            dm_embed.set_footer(text=f"Server ID: {server_id} | VMID: {next_vmid} | Node: {target_node_id}")

            dm_sent_successfully = False
            try:
                await interaction.user.send(embed=dm_embed)
                dm_sent_successfully = True
            except discord.HTTPException:
                await interaction.followup.send(
                    f"{Elookup('success')} VPS **{server_name_resp}** created for you! However, I couldn't DM you the details. **Please see them below and save them securely:**", 
                    embed=dm_embed, ephemeral=False
                )
            
            if dm_sent_successfully:
                await send_success_embed(interaction, "VPS Approved & Created!", f"Your **{plan_name_log}** VPS request was approved and the server is now ready! Please check your DMs for the server details.", ephemeral=True)
            
            # Log the successful creation
            await send_vps_log(f"User Approved Creation ({plan_name_log})", interaction.user, dm_embed, server_id, server_id, server_name_resp)
        
        else:  # API creation failed
            await send_error_embed(interaction, "VPS Creation Failed Post-Approval", "Your VPS request was approved by an admin, but an error occurred during the final server creation process on the panel. Please contact an administrator for assistance. The reserved IP was not used.")
            if admin_conf_view.response_admin:
                try: 
                    await admin_conf_view.response_admin.send(f"⚠️ **CRITICAL FAILURE:** VPS creation FAILED for user {interaction.user.mention} (Plan: {plan_name_log}) **after your approval**. Please investigate the panel logs. The IP `{assigned_ip_from_file}` should be manually returned to `{IP_FILENAME}`.")
                except discord.HTTPException: 
                    pass
            try:
                with open(IP_FILENAME, 'a') as f: 
                    f.write(f"\n{assigned_ip_from_file}\n")
                logger.info(f"Returned IP '{assigned_ip_from_file}' to {IP_FILENAME} due to post-approval creation failure.")
            except IOError: 
                logger.error(f"Failed to return IP '{assigned_ip_from_file}' to {IP_FILENAME} after creation failure.")

    elif admin_conf_view.status is False:  # Denied
        try:
            await interaction.user.send(embed=discord.Embed(
                title=f"{Elookup('error')} VPS Request Denied",
                description=f"Unfortunately, your request for the **{plan_name_log}** VPS was denied by administrator {admin_conf_view.response_admin.mention}. If you have questions, please open a support ticket.",
                color=discord.Color.red()
            ))
        except discord.HTTPException:
            await interaction.followup.send("Your VPS creation request was denied by an administrator. The reserved IP was not used.", ephemeral=True)
        try:
            with open(IP_FILENAME, 'a') as f: 
                f.write(f"\n{assigned_ip_from_file}\n")
            logger.info(f"Returned IP '{assigned_ip_from_file}' to {IP_FILENAME} as request was denied.")
        except IOError: 
            logger.error(f"Failed to return IP '{assigned_ip_from_file}' to {IP_FILENAME} after denial.")
    
    else:  # Timeout
        try:
            await interaction.user.send(embed=discord.Embed(
                title=f"{Elookup('loading')} VPS Request Timed Out",
                description=f"Your request for the **{plan_name_log}** VPS has timed out as no administrator action was taken within the allowed time. You can try submitting the request again later. The reserved IP was not used.",
                color=discord.Color.dark_gray()
            ))
        except discord.HTTPException:
            await interaction.followup.send("Your VPS creation request timed out due to no administrator action. The reserved IP was not used.", ephemeral=True)
        try:
            with open(IP_FILENAME, 'a') as f: 
                f.write(f"\n{assigned_ip_from_file}\n")
            logger.info(f"Returned IP '{assigned_ip_from_file}' to {IP_FILENAME} as request timed out.")
        except IOError: 
            logger.error(f"Failed to return IP '{assigned_ip_from_file}' to {IP_FILENAME} after timeout.")

# --- Purge Command ---
@admin_group.command(name="purge", description="🗑️ [DANGEROUS] Delete ALL servers from the panel.")
@is_bot_owner()
async def admin_purge_servers(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    
    confirm_view = ConfirmView(
        interaction.user.id, 
        timeout=60,
        confirm_label="YES, DELETE ALL SERVERS",
        cancel_label="CANCEL",
        confirm_style=discord.ButtonStyle.red
    )
    
    servers_data = await make_api_request('GET', '/servers', 'application', interaction, params={'per_page': 1})
    total_servers = 0
    if servers_data and isinstance(servers_data.get('data'), list):
        total_servers = servers_data.get('meta', {}).get('pagination', {}).get('total', 0)
    
    await interaction.followup.send(
        f"🚨 **CRITICAL PURGE OPERATION** 🚨\n\n"
        f"You are about to delete **ALL {total_servers} SERVERS** from the panel!\n\n"
        f"❌ **THIS ACTION CANNOT BE UNDONE!**\n"
        f"❌ **ALL SERVER DATA WILL BE PERMANENTLY LOST!**\n"
        f"❌ **BACKUPS AND SNAPSHOTS WILL ALSO BE DELETED!**\n\n"
        f"Are you absolutely sure you want to proceed with this mass deletion?",
        view=confirm_view, ephemeral=True
    )
    confirm_view.message = await interaction.original_response()
    await confirm_view.wait()

    if confirm_view.confirmed:
        await confirm_view.message.edit(content=f"{Elookup('loading')} Starting mass server deletion... This may take a while.", view=None)
        
        deleted_count = 0
        error_count = 0
        page = 1
        
        while True:
            servers_response = await make_api_request('GET', '/servers', 'application', interaction, params={'page': page, 'per_page': 50})
            
            if not servers_response or not isinstance(servers_response.get('data'), list) or not servers_response['data']:
                break
                
            servers = servers_response['data']
            
            for server in servers:
                server_id = server.get('id')
                server_name = server.get('name', 'Unknown Server')
                
                if server_id:
                    delete_response = await make_api_request('DELETE', f'/servers/{server_id}', 'application', interaction)
                    if delete_response is not None:
                        deleted_count += 1
                        logger.info(f"Deleted server: {server_name} (ID: {server_id})")
                    else:
                        error_count += 1
                        logger.error(f"Failed to delete server: {server_name} (ID: {server_id})")
            
            page += 1
            
            if len(servers) < 50:
                break
        
        result_message = f"{Elookup('success')} Purge operation completed!\n\n"
        result_message += f"✅ **Successfully deleted:** {deleted_count} servers\n"
        if error_count > 0:
            result_message += f"❌ **Failed to delete:** {error_count} servers\n"
        
        await confirm_view.message.edit(content=result_message, view=None)
    else:
        await confirm_view.message.edit(content=f"{Elookup('cancel')} Purge operation cancelled.", view=None)

# --- Logging Helper ---
async def send_vps_log(log_type: str, interaction_user: discord.User, details_embed: discord.Embed, server_uuid: str, server_short_id: str, server_name: str):
    if not VPS_LOG_CHANNEL_ID: 
        return
    log_channel = bot.get_channel(VPS_LOG_CHANNEL_ID)
    if not isinstance(log_channel, discord.TextChannel):
        logger.error(f"VPS log channel ({VPS_LOG_CHANNEL_ID}) is not a valid Text Channel.")
        return

    log_embed = details_embed.copy()
    log_embed.title = f"{Elookup('server', '📄')} {log_type} Log: {server_name}"
    log_embed.set_author(name=f"Initiated by: {interaction_user.display_name} ({interaction_user.id})", icon_url=interaction_user.display_avatar.url if interaction_user.display_avatar else None)
    log_embed.timestamp = datetime.now(timezone.utc)
    
    try:
        await log_channel.send(embed=log_embed)
        logger.info(f"Logged {log_type} for server {server_short_id} initiated by {interaction_user.id}")
    except discord.Forbidden:
        logger.error(f"Bot missing permissions to send message in log channel {VPS_LOG_CHANNEL_ID}")
    except discord.HTTPException as e:
        logger.error(f"Failed to send log message to channel {VPS_LOG_CHANNEL_ID}: {e}")

# --- Basic Commands ---
@bot.tree.command(name="link", description=f"{Elookup('link')} Links your Discord account to your Panel user account.")
async def link_account_cmd(interaction: discord.Interaction): 
    existing_link_id = await get_linked_convoy_id(interaction.user.id)
    if existing_link_id:
        await send_info_embed(interaction, "Account Already Linked", f"Your Discord account is already linked to panel account ID: **{existing_link_id}**.", ephemeral=True)
        return
        
    # Simple linking by user ID
    user_data_response = await make_api_request('GET', f'/users/{interaction.user.id}', 'application', interaction)
    
    if user_data_response and isinstance(user_data_response, dict):
        convoy_panel_user_id = user_data_response.get('id')
        if convoy_panel_user_id:
            await link_user_account(interaction.user.id, str(convoy_panel_user_id))
            await send_success_embed(interaction, "Account Successfully Linked!", f"{Elookup('success')} Your Discord account is now linked to the panel user ID: `{convoy_panel_user_id}`.", ephemeral=True)
        else:
            await send_error_embed(interaction, "User Not Found", f"No panel user account was found with ID matching your Discord ID. Please contact an administrator.", ephemeral=True)
    else:
        await send_error_embed(interaction, "Linking Error", "Could not verify your panel account. Please ensure you have an account on the panel and contact an administrator if the issue persists.", ephemeral=True)

@bot.tree.command(name="servers", description=f"{Elookup('server','📄')} Lists all servers linked to your panel account.")
async def list_my_servers(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=False)
    user_panel_id = await get_linked_convoy_id(interaction.user.id)
    if not user_panel_id:
        await send_error_embed(interaction, "Account Not Linked", "Your Discord account is not linked to a panel account. Please use `/link` first.", ephemeral=True)
        return
    
    params = {'page': 1, 'per_page': 5, 'filter[user_id]': user_panel_id}
    response_data = await make_api_request('GET', '/servers', 'application', interaction, params=params)
   
    if response_data and isinstance(response_data.get('data'), list):
        if not response_data['data'] and response_data.get('meta', {}).get('pagination', {}).get('total', 0) == 0:
            await send_info_embed(interaction, "My Servers", "🍃 You currently don't have any servers linked to your account on the panel.", ephemeral=False)
            return
        view = ServerListView(interaction, response_data, items_per_page=5, title_prefix="My Servers", api_type='application')
        view.user_panel_id_filter = user_panel_id
        await view.start()
    elif response_data is not None:
        if not interaction.response.is_done():
            await send_error_embed(interaction, "API Format Error", f"Received unexpected data format when fetching your servers.", ephemeral=True)

@bot.tree.command(name="credits", description=f"{Elookup('credits')} Shows bot creator and project credits.")
async def credits_cmd(interaction: discord.Interaction): 
    embed = discord.Embed(title=f"{Elookup('credits')} RN Nodes Bot - Credits", description="This bot was created for the RN Nodes community.", color=discord.Color.gold())
    embed.add_field(name="🚀 RN Nodes Project", value="[Visit our Website](https://rn-nodes.pro)", inline=False)
    if bot.user and bot.user.avatar:
        embed.set_thumbnail(url=bot.user.avatar.url)
    embed.set_footer(text="Thank you for using the RN Nodes Bot!")
    await interaction.response.send_message(embed=embed, ephemeral=False)

# --- Bot Events ---
@bot.event
async def on_ready():
    logger.info(f"Bot Logged In: {bot.user.name} (ID: {bot.user.id})")
    logger.info(f"Discord.py Version: {discord.__version__}")
    logger.info(f"Connected to {len(bot.guilds)} guild(s).")
    logger.info(f"Panel Base URL: {PANEL_BASE_URL}")
    logger.info(f"RN Nodes Bot is now online!")
    
    activity_conf = config.get('bot_activity', {})
    activity_name = activity_conf.get("name", "RN Nodes")
    activity_type_str = activity_conf.get("type", "watching").lower()

    activity_map = {
        "playing": discord.ActivityType.playing, "streaming": discord.ActivityType.streaming, 
        "listening": discord.ActivityType.listening, "watching": discord.ActivityType.watching,
        "competing": discord.ActivityType.competing
    }
    selected_activity_type = activity_map.get(activity_type_str, discord.ActivityType.watching)
    
    try:
        await bot.change_presence(activity=discord.Activity(name=activity_name, type=selected_activity_type))
        logger.info(f"Bot presence set to: {selected_activity_type.name.capitalize()} {activity_name}")
    except Exception as e:
        logger.error(f"Failed to set bot presence: {e}")

@bot.event
async def on_member_join(member: discord.Member):
    if not member.guild or member.bot: 
        return
    
    await asyncio.sleep(INVITE_CHECK_DELAY_SECONDS) 
    
    try:
        current_guild_invites = await member.guild.invites()
    except discord.Forbidden: 
        return
    except discord.HTTPException as e: 
        logger.error(f"HTTPException while fetching invites for guild {member.guild.name} ({member.guild.id}): {e}")
        return

    cached_guild_invites_map = guild_invite_cache.get(member.guild.id, {})
    found_inviter_user: Optional[discord.User] = None

    for new_invite_obj in current_guild_invites:
        if new_invite_obj.code is None or new_invite_obj.uses is None or new_invite_obj.inviter is None: 
            continue
        
        if new_invite_obj.code not in cached_guild_invites_map or \
           new_invite_obj.uses > cached_guild_invites_map.get(new_invite_obj.code, -1):
            
            if found_inviter_user is None:
                found_inviter_user = new_invite_obj.inviter
            else:
                logger.warning(f"Ambiguous inviter detection for new member {member.name} in guild {member.guild.name}.")
                found_inviter_user = None
                break 
    
    if found_inviter_user and found_inviter_user.id != member.id:
        increment_invite_count(member.guild.id, found_inviter_user.id)
        logger.info(f"Attributed join of {member.name} ({member.id}) in guild {member.guild.name} to inviter {found_inviter_user.name} ({found_inviter_user.id}).")

    guild_invite_cache[member.guild.id] = {inv.code: inv.uses for inv in current_guild_invites if inv.code and inv.uses is not None}

# --- Run Bot ---
if __name__ == "__main__":
    if not DISCORD_TOKEN:
        logger.critical("FATAL: Discord token (DISCORD_TOKEN) not found.")
        exit(1)
    try:
        logger.info("RN Nodes Bot is preparing to launch...")
        bot.run(DISCORD_TOKEN, log_handler=None)
    except discord.LoginFailure:
        logger.critical("FATAL: Discord login failed. Please check if the DISCORD_TOKEN is correct and valid.")
    except Exception as e:
        logger.critical(f"FATAL: An unexpected error occurred while trying to run the bot: {e}", exc_info=True)
