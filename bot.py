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
IP_FILENAME = "ips.txt" # For get_and_remove_first_ip

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

# Emoji lookup helper
def Elookup(key: str, default: str = "❓") -> str:
    return config.get('emojis', {}).get(key, default)

# --- Essential Config Validation ---
DISCORD_TOKEN = config.get('discord_token')
CONVOY_API_URL_BASE = config.get('convoy_api_url', '').rstrip('/')
CONVOY_APP_API_KEY = config.get('convoy_api_key') # For Application API
CONVOY_CLIENT_API_KEY = config.get('client_api_key') # For Client API
VPS_CREATOR_ROLE_ID = config.get('vps_creator_role_id')
BOT_OWNER_USER_ID = config.get('bot_owner_user_id')
PANEL_BASE_URL = config.get('panel_base_url', CONVOY_API_URL_BASE).rstrip('/')

VPS_LOG_CHANNEL_ID = config.get('channel_ids', {}).get('vps_log')
ADMIN_VPS_APPROVAL_CHANNEL_ID = config.get('channel_ids', {}).get('admin_vps_approval')

CONVOY_APP_API_URL = f"{CONVOY_API_URL_BASE}/api/application"
CONVOY_CLIENT_API_URL = f"{CONVOY_API_URL_BASE}/api/client"


if not all([DISCORD_TOKEN, CONVOY_API_URL_BASE, CONVOY_APP_API_KEY, CONVOY_CLIENT_API_KEY, VPS_CREATOR_ROLE_ID, BOT_OWNER_USER_ID]):
    logger.critical("Missing required fields in config.json: discord_token, convoy_api_url, convoy_api_key, client_api_key, vps_creator_role_id, bot_owner_user_id")
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

# Reward Toggles & Data (Load from new config structure)
BOOST_REWARDS_ENABLED = config.get('reward_plans_enabled', {}).get('boost', False)
INVITE_REWARDS_ENABLED_GLOBAL = config.get('reward_plans_enabled', {}).get('invite', False) # Global toggle for invites

BOOST_REWARD_TIERS = config.get('boost_reward_tiers', [])
INVITE_REWARD_TIERS = config.get('invite_reward_tiers', [])
PAID_PLANS_DATA = config.get('paid_plans_data', []) # <<< ADDED FOR /plans COMMAND

# Defaults from config
DEFAULT_NODE_ID = config.get('defaults', {}).get('node_id')
DEFAULT_TEMPLATE_UUID = config.get('defaults', {}).get('template_uuid')
DEFAULT_USER_SNAPSHOT_LIMIT = config.get('defaults', {}).get('user_snapshot_limit', 1)
DEFAULT_USER_BACKUP_LIMIT = config.get('defaults', {}).get('user_backup_limit', 1)
DEFAULT_ADMIN_SNAPSHOT_LIMIT = config.get('defaults', {}).get('admin_snapshot_limit', 2)
DEFAULT_ADMIN_BACKUP_LIMIT = config.get('defaults', {}).get('admin_backup_limit', 0)
DEFAULT_SERVER_HOSTNAME_SUFFIX = config.get('defaults', {}).get('default_server_hostname_suffix', 'rnhost.pro')

NODE_IPS_MAP = config.get('node_ips_map', {}) # For /nodes command
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
        super().__init__(command_prefix="thisisnotused!", intents=intents)
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
        logger.info("Shutting down Rn Nodes bot...")
        await super().close()
        if self.http_session:
            await self.http_session.close()
            logger.info("Closing connection to Panel API...")

    async def cache_invites_periodically(self, interval_seconds=300):
        await self.wait_until_ready()
        while not self.is_closed():
            # logger.info(f"[{datetime.now():%H:%M:%S}] Caching invites...")
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

# --- API Jokes (kept for /helptroll) ---
api_jokes = [
    "Why did the API break up with the database? It had too many commitment issues! 😂", "Why don't APIs ever get lonely? Because they always have endpoints! 🤝",
    "What do you call a lazy API? RESTful! 😴", "Why was the API key always calm? It knew how to handle requests.🧘",
    "How do APIs stay in shape? By running endpoints! 🏃‍♀️", "Why did the developer bring a ladder to the API meeting? To reach the high-level endpoints! 🪜",
    "What's an API's favorite type of music? Heavy Metal... because it handles a lot of requests! 🤘", "Why was the JSON data always invited to parties? Because it knew how to structure things! 🎉",
    "How does an API apologize? It sends a '418 I'm a teapot' status! 🫖", "What did the API say to the UI? 'You complete me!' ❤️",
] * 5
def get_random_api_joke(): return random.choice(api_jokes)


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
                if stripped_line and not stripped_line.startswith('#'): # Ignore empty lines and comments
                    found_ip = stripped_line
                    ip_found_and_skipped = True
                else:
                    pass # Skip leading empty/comment lines or already processed lines
            else:
                lines_to_keep.append(line) # Keep lines after the found IP

        if found_ip:
            try:
                with open(filename, 'w') as f:
                    f.writelines(lines_to_keep)
                logger.info(f"Successfully used IP '{found_ip}' and updated '{filename}'.")
            except IOError as e:
                logger.error(f"Error writing back to file '{filename}': {e}")
                # Potentially re-add the IP to the list if write fails? Or log critical.
        else:
             logger.warning(f"No valid IP address found in '{filename}'. File not modified.")
    except FileNotFoundError: # Should be caught by the exists check, but good fallback
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
    method: str, endpoint: str, api_type: str, # 'application' or 'client'
    interaction: Optional[discord.Interaction],
    json_data: Optional[dict] = None, params: Optional[dict] = None
) -> Optional[Union[dict, list, Any]]:
    if not bot.http_session or bot.http_session.closed:
        if bot.http_session and bot.http_session.closed:
            logger.warning("HTTP session was closed, attempting to recreate.")
            bot.http_session = aiohttp.ClientSession(headers={"User-Agent": "ConvoyDiscordBot/1.1"})
        else:
            logger.error("Bot HTTP session not initialized.")
            if interaction and not interaction.response.is_done(): await send_error_embed(interaction, "Bot Error", "HTTP session not ready.")
            return None

    base_url = CONVOY_APP_API_URL if api_type == 'application' else CONVOY_CLIENT_API_URL
    api_key = CONVOY_APP_API_KEY if api_type == 'application' else CONVOY_CLIENT_API_KEY
    
    url = f"{base_url}/{endpoint.lstrip('/')}"
    headers = {
        'Authorization': f'Bearer {api_key}',
        'Content-Type': 'application/json',
        'Accept': 'application/json',
    }

    # logger.debug(f"API Call ({api_type}): {method} {url} | Params: {params} | Data: {str(json_data)[:200]}")

    try:
        async with bot.http_session.request(method, url, headers=headers, json=json_data, params=params, timeout=aiohttp.ClientTimeout(total=45)) as response:
            response_text = await response.text()
            # logger.debug(f"Response Status ({api_type} {endpoint}): {response.status} | Body: {response_text[:300]}...")

            if 200 <= response.status < 300:
                if response.status == 204: return {"status_code": 204, "message": "Action completed."}
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
        if interaction and not interaction.response.is_done(): await send_error_embed(interaction, f"{Elookup('network')} Network Error", f"Could not connect to Panel API at `{CONVOY_API_URL_BASE}`.")
        return None
    except asyncio.TimeoutError:
        logger.error(f"Timeout Error for {method} {url}")
        if interaction and not interaction.response.is_done(): await send_error_embed(interaction, f"{Elookup('loading')} Network Timeout", f"Request to Panel API timed out.")
        return None
    except Exception as e:
        logger.error(f"Unexpected API Request Error ({api_type} {endpoint}): {e}", exc_info=True)
        if interaction and not interaction.response.is_done(): await send_error_embed(interaction, f"{Elookup('error')} Bot Error", "Unexpected error talking to the API.")
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
def load_linked_accounts_sync() -> Dict[str, str]: return load_json_file(LINKED_ACCOUNTS_FILE, {})
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
    if gid_str not in invite_counts: invite_counts[gid_str] = {}
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
            if isinstance(item, discord.ui.Button): item.disabled = True
        try:
            if interaction and not interaction.response.is_done():
                await interaction.response.edit_message(view=self)
            elif self.message:
                await self.message.edit(view=self)
        except discord.HTTPException: pass


    async def confirm_button_callback(self, interaction: discord.Interaction):
        self.confirmed = True
        await self._disable_all_buttons(interaction)
        try:
            await interaction.edit_original_response(content=f"{Elookup('confirm')} Confirmed. Processing...", view=self)
        except discord.HTTPException:
            if self.message: await self.message.edit(content=f"{Elookup('confirm')} Confirmed. Processing...", view=self)
        self.stop()

    async def cancel_button_callback(self, interaction: discord.Interaction):
        self.confirmed = False
        await self._disable_all_buttons(interaction)
        try:
            await interaction.edit_original_response(content=f"{Elookup('cancel')} Action cancelled.", view=self)
        except discord.HTTPException:
            if self.message: await self.message.edit(content=f"{Elookup('cancel')} Action cancelled.", view=self)
        self.stop()

    async def on_timeout(self):
        if self.confirmed is None: # No action taken
            await self._disable_all_buttons()
            if self.message:
                try: await self.message.edit(content=f"{Elookup('loading')} Confirmation timed out.", view=self)
                except discord.HTTPException: pass

# --- Log Action View (For Logging Channel) ---
class LogActionView(discord.ui.View):
    def __init__(self, server_uuid: str, server_short_id: str, server_name: str):
        super().__init__(timeout=None) # Persistent
        self.server_uuid = server_uuid
        self.server_short_id = server_short_id # This is likely the Application API internal ID
        self.server_name = server_name

        # Use server_short_id for actions that typically use it in Application API (suspend, delete)
        suspend_button = discord.ui.Button(label="Suspend", style=discord.ButtonStyle.secondary, custom_id=f"log_suspend:{server_short_id}", emoji=Elookup("power_off"))
        suspend_button.callback = self.suspend_callback
        self.add_item(suspend_button)

        delete_button = discord.ui.Button(label="Delete", style=discord.ButtonStyle.danger, custom_id=f"log_delete:{server_short_id}", emoji=Elookup("delete"))
        delete_button.callback = self.delete_callback
        self.add_item(delete_button)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if not check_is_vps_creator(interaction):
            await interaction.response.send_message(f"{Elookup('error')} Only VPS Creators can perform actions from the log.", ephemeral=True)
            return False
        return True

    async def suspend_callback(self, interaction: discord.Interaction):
        server_id_to_action = interaction.data['custom_id'].split(':')[1] # Get the ID from button
        await interaction.response.defer(ephemeral=True)
        response = await make_api_request('POST', f'/servers/{server_id_to_action}/suspend', 'application', interaction)
        if response:
            await interaction.followup.send(f"{Elookup('success')} Initiated suspend for **{self.server_name}** (ID: `{server_id_to_action}`).", ephemeral=True)
            # If successful, you might want to update the button to "Unsuspend" or disable it
            # For simplicity, this is omitted here.

    async def delete_callback(self, interaction: discord.Interaction):
        server_id_to_action = interaction.data['custom_id'].split(':')[1] # Get the ID from button
        confirm_view = ConfirmView(interaction.user.id, confirm_label="Yes, Delete Server")
        await interaction.response.send_message(
            f"{Elookup('warning')} **Confirm Deletion:** Delete **{self.server_name}** (ID: `{server_id_to_action}`)? **CANNOT BE UNDONE**.",
            view=confirm_view, ephemeral=True
        )
        confirm_view.message = await interaction.original_response()
        await confirm_view.wait()

        if confirm_view.confirmed:
            processing_msg = await interaction.followup.send(f"{Elookup('loading')} Processing deletion...", ephemeral=True)
            response = await make_api_request('DELETE', f'/servers/{server_id_to_action}', 'application', interaction)
            if response:
                await processing_msg.edit(content=f"{Elookup('success')} Initiated delete for **{self.server_name}** (ID: `{server_id_to_action}`).")
                # Disable buttons on the log message as server is gone
                if self.view_message: # If view stores its original message
                    for item in self.children: item.disabled = True
                    try: await self.view_message.edit(view=self)
                    except: pass


# --- Logging Helper ---
async def send_vps_log(log_type: str, interaction_user: discord.User, details_embed: discord.Embed, server_uuid: str, server_short_id: str, server_name: str):
    if not VPS_LOG_CHANNEL_ID: return
    log_channel = bot.get_channel(VPS_LOG_CHANNEL_ID)
    if not isinstance(log_channel, discord.TextChannel):
        logger.error(f"VPS log channel ({VPS_LOG_CHANNEL_ID}) is not a valid Text Channel.")
        return

    log_embed = details_embed.copy()
    log_embed.title = f"{Elookup('server', '📄')} {log_type} Log: {server_name}"
    log_embed.set_author(name=f"Initiated by: {interaction_user.display_name} ({interaction_user.id})", icon_url=interaction_user.display_avatar.url if interaction_user.display_avatar else None)
    log_embed.timestamp = datetime.now(timezone.utc)
    
    # Pass server_short_id to LogActionView if actions use it, or server_uuid if they use that
    view = LogActionView(server_uuid, server_short_id, server_name)
    try:
        log_msg = await log_channel.send(embed=log_embed, view=view)
        if hasattr(view, 'view_message'): # If LogActionView stores its message for disabling buttons
            view.view_message = log_msg
        logger.info(f"Logged {log_type} for server {server_short_id} initiated by {interaction_user.id}")
    except discord.Forbidden:
        logger.error(f"Bot missing permissions to send message in log channel {VPS_LOG_CHANNEL_ID}")
    except discord.HTTPException as e:
        logger.error(f"Failed to send log message to channel {VPS_LOG_CHANNEL_ID}: {e}")


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
            if new_max_page == 0 and self.total_items > 0 :
                self.max_page = math.ceil(self.total_items / self.items_per_page) if self.total_items > 0 else 1
            elif new_max_page > 0: self.max_page = new_max_page
            else: self.max_page = 1 # Fallback if total_pages is 0 but items exist
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
                cpu_cores = limits.get('cpu', 'N/A') # This is cores/percentage for Convoy
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
        if self.title_prefix == "All Panel Servers": # Only show for admin list
            footer_text += " | Use /admin assign to change owner."
        embed.set_footer(text=footer_text)
        return embed

    def _update_button_states(self):
        self._prev_button.disabled = self.current_page <= 1
        self._next_button.disabled = self.current_page >= self.max_page
        self._page_indicator.label = f"Page {self.current_page}/{self.max_page}"
        if self.total_items == 0: # Disable all if no items
            self._prev_button.disabled = True
            self._next_button.disabled = True


    async def _update_view_message(self, content: Optional[str] = None, embed: Optional[discord.Embed] = None, view: Optional[discord.ui.View] = None):
        if not self.view_message: return
        try:
            await self.view_message.edit(content=content, embed=embed, view=view if view is not None else self)
        except discord.HTTPException as e:
            logger.error(f"ServerListView: Failed to edit view_message (ID: {self.view_message.id}): {e}")

    async def _previous_button_callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.original_command_interaction.user.id:
            return await interaction.response.send_message(f"{Elookup('error')} This isn't your menu!", ephemeral=True)
        await interaction.response.defer()
        if self.current_page > 1:
            await self._update_view_message(content=f"{Elookup('loading')} Fetching previous page...", embed=None, view=None) # Clear embed during load
            if await self._fetch_page_data(self.current_page - 1, interaction): 
                self._update_button_states()
                await self._update_view_message(embed=self._create_page_embed())
            else: # Fetch failed, restore current page view
                self._update_button_states() # Current state might be more accurate now
                await self._update_view_message(content=None, embed=self._create_page_embed()) # Re-render current page

    async def _next_button_callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.original_command_interaction.user.id:
            return await interaction.response.send_message(f"{Elookup('error')} This isn't your menu!", ephemeral=True)
        await interaction.response.defer()
        if self.current_page < self.max_page:
            await self._update_view_message(content=f"{Elookup('loading')} Fetching next page...", embed=None, view=None) # Clear embed during load
            if await self._fetch_page_data(self.current_page + 1, interaction):
                self._update_button_states()
                await self._update_view_message(embed=self._create_page_embed())
            else: # Fetch failed
                self._update_button_states()
                await self._update_view_message(content=None, embed=self._create_page_embed())

    async def start(self):
        self._update_button_states()
        embed = self._create_page_embed()
        try:
            # Send non-ephemeral for server lists that users might want to share/see
            await self.original_command_interaction.followup.send(embed=embed, view=self, ephemeral=False) 
            self.view_message = await self.original_command_interaction.original_response()
        except discord.HTTPException as e:
            logger.error(f"ServerListView: Failed to send initial '{self.title_prefix}' list: {e}")
            try: await self.original_command_interaction.followup.send(f"{Elookup('error')} Error displaying {self.title_prefix.lower()} list.", ephemeral=True)
            except: pass # Best effort

    async def on_timeout(self):
        if not self.view_message:
            try: self.view_message = await self.original_command_interaction.original_response()
            except discord.HTTPException: return # No message to edit
        
        for item in self.children:
            if isinstance(item, discord.ui.Button): item.disabled = True
        
        final_embed = self._create_page_embed() # Show last known state
        final_embed.set_footer(text=f"Total Servers: {self.total_items} | View timed out {Elookup('loading')}")
        await self._update_view_message(embed=final_embed)

@admin_group.command(name="serverlist", description=f"{Elookup('server', '📄')} List all servers on the panel (paginated).")
@is_vps_creator()
async def admin_list_all_servers(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=False) 
    params = {'page': 1, 'per_page': 5} # Initial fetch for 5 items
    response_data = await make_api_request('GET', '/servers', 'application', interaction, params=params)

    if response_data and isinstance(response_data.get('data'), list):
        if not response_data['data'] and response_data.get('meta', {}).get('pagination', {}).get('total', 0) == 0:
            await send_info_embed(interaction, "List All Servers", "🍃 No servers found on the panel.", ephemeral=False)
            return
        view = ServerListView(interaction, response_data, items_per_page=5, title_prefix="All Panel Servers", api_type='application')
        await view.start() 
    elif response_data is not None: # API call was made, but data is not as expected
        if not interaction.response.is_done(): # Check because make_api_request might have sent error
            await send_error_embed(interaction, "API Format Error", f"Received unexpected data format from the API: ```{str(response_data)[:1000]}```", ephemeral=True)
    # If response_data is None, make_api_request already handled sending an error embed

@admin_group.command(name="assign", description=f"{Elookup('user')} Assign a server to a different Discord user.")
@is_vps_creator()
@app_commands.describe(server_identifier="The short ID (Application API ID) or UUID of the server.", user="The Discord user to assign the server to.")
async def admin_assign_server(interaction: discord.Interaction, server_identifier: str, user: discord.User):
    await interaction.response.defer(ephemeral=True)
    target_convoy_id = await get_linked_convoy_id(user.id)
    if not target_convoy_id:
        await send_error_embed(interaction, "User Not Linked", f"{user.mention} has not linked their Discord account using `/link`.")
        return

    # Fetch server details to confirm it exists and get its Application API ID if UUID was provided
    # Convoy/Convoy application API GET /api/application/servers/{external_id} works with UUID too
    # Or GET /api/application/servers/{internal_id}
    # Let's assume server_identifier can be either. If it's UUID, we need internal_id for PATCH.
    
    # First try fetching by server_identifier as if it's the internal app ID
    server_details_resp = await make_api_request('GET', f'/servers/{server_identifier}', 'application', interaction)
    
    # If not found, and it looks like a UUID, try fetching specifically by UUID
    # (Convoy typically uses internal IDs for most app API endpoints like PATCH)
    # Standard Convoy: GET /api/application/servers/external/{uuid} exists.
    # If your "Convoy" panel supports this, you can use it. Otherwise, you'd need to list all servers and filter.
    # For simplicity, we'll assume GET /servers/{server_identifier} works for both if the panel is flexible
    # or that admin provides the internal ID.

    if not server_details_resp or not isinstance(server_details_resp.get('data'), dict):
        if not interaction.response.is_done():
            await send_error_embed(interaction, "Server Not Found", f"Could not find server details for identifier `{server_identifier}`.")
        return

    server_data = server_details_resp['data']
    server_internal_id = server_data.get('id') # This is the Application API internal ID
    server_name_original = server_data.get('name', server_identifier)

    if not server_internal_id:
        await send_error_embed(interaction, "API Error", "Could not retrieve the internal ID for the server needed for assignment.")
        return

    payload = {"user_id": int(target_convoy_id)} # Convoy/Convoy: "user"
    # Convoy uses PATCH /api/application/servers/{internal_id}/details for user/name changes
    # Convoy might just use PATCH /api/application/servers/{internal_id}
    
    # Attempting common Convoy endpoint first
    assign_response = await make_api_request('PATCH', f'/servers/{server_internal_id}/details', 'application', interaction, json_data=payload)
    
    if not assign_response: # If /details failed, try the simpler endpoint common in some Convoy forks/Convoy
        logger.info(f"PATCH /servers/{server_internal_id}/details failed for assignment, trying PATCH /servers/{server_internal_id}")
        assign_response = await make_api_request('PATCH', f'/servers/{server_internal_id}', 'application', interaction, json_data=payload)

    if assign_response and isinstance(assign_response.get('data'), dict) :
        updated_server_data = assign_response['data']
        updated_name = updated_server_data.get('name', server_name_original)
        assigned_user_id = updated_server_data.get('user_id', "N/A")
        await send_success_embed(interaction, "Server Assigned", f"Successfully assigned server **{updated_name}** (ID: `{server_internal_id}`) to {user.mention} (Panel User ID: `{assigned_user_id}`).", ephemeral=True)
    elif assign_response: # Response received but not the expected format
         if not interaction.response.is_done():
            await send_error_embed(interaction, "Assignment Error", f"Assignment command sent, but unexpected API response for server `{server_internal_id}`. Verify assignment on panel.")
    # If assign_response is None, make_api_request handled the error.

# Assume admin_group is an existing app_commands.Group instance
# admin_group = app_commands.Group(name="admin", description="Admin commands.")

@admin_group.command(name="createuser", description="➕ Create a user account on the Convoy panel.") # Command name is 'createuser'
@is_vps_creator() # Assuming this decorator checks permissions and sends an error message if needed
@app_commands.describe(discord_user="The Discord user to create a panel account for.",
                       email="The email address for the panel account.",
                       is_admin="Should this user be a root admin on the panel? (Default: False)")
async def create_user(interaction: discord.Interaction, discord_user: discord.User, email: str, is_admin: bool = False):
    """
    Creates a user account on the Convoy panel for a specified Discord user,
    links the accounts, and DMs the user their temporary password.
    """
    # Defer the interaction response, initially ephemeral (only visible to the admin)
    await interaction.response.defer(ephemeral=True)

    # Check if the target Discord user already has a linked panel account
    if await get_linked_convoy_id(discord_user.id):
         # If already linked, send an info embed and exit the command
         # Ensure the response is ephemeral
         await send_info_embed(interaction, "User Already Linked", f"{discord_user.mention} already has a linked panel account.", ephemeral=True)
         return

    # Generate a secure temporary password for the new panel account
    # Requires 'import string' and 'import random' at the top of your full script
    characters = string.ascii_letters + string.digits + string.punctuation
    password = ''.join(random.choice(characters) for i in range(16)) # Generate 16 characters

    # Prepare the payload dictionary for the API request to create the user
    payload = {
        "name": str(discord_user.display_name)[:191], # Use Discord display name, truncated if needed for panel limit
        "email": email, # Use the provided email
        "password": password, # Use the generated temporary password
        "root_admin": is_admin, # Set admin status based on the command argument
    }

    # Make the API request to create the user on the panel
    # Uses the make_api_request helper function defined elsewhere
    # The '/users' endpoint requires the 'application' API type
    # make_api_request is assumed to handle network errors and API non-2xx responses
    response_data = await make_api_request('POST', '/users', 'application', interaction, json_data=payload)

    # Process the API response only if it was successful and returned data in the expected format
    if response_data and isinstance(response_data.get('data'), dict):
        created_user_data = response_data['data']
        convoy_user_id = created_user_data.get('id') # Get the panel user ID from the response
        # Get the panel username/name, preferring 'username' if available
        panel_username = created_user_data.get('username', created_user_data.get('name', 'N/A'))

        # If the API response didn't contain a user ID, something went wrong despite a 2xx status
        if not convoy_user_id:
             # Send an error embed to the admin user via interaction followup. Ensure ephemeral.
             await send_error_embed(interaction, "API Error", "User created on panel, but failed to get user ID from API response. Cannot link account.", ephemeral=True)
             # Log a warning with response details for debugging
             print(f"Warning: Created user {panel_username} ({email}) but couldn't get ID. API response: {response_data}")
             # Exit the command as account linking failed
             return

        # If a panel user ID was successfully retrieved, link the Discord user ID to the panel user ID
        # Ensure link_user_account handles the convoy_user_id as a string if necessary
        await link_user_account(discord_user.id, str(convoy_user_id))

        # Prepare the message to send to the target Discord user via DM
        # Includes panel login details and temporary password
        success_msg_dm = (
            f"Your Rn Nodes panel account has been created!\n\n"
            f"**Panel Username:** `{panel_username}`\n"
            f"**Panel Email:** `{email}`\n"
            f"**Panel Link:** <{PANEL_BASE_URL}>\n\n"
            f"🤫 **Temporary Password:** ||`{password}`||\n\n" # Password in a spoiler tag for mild security
            f"**Important:** Please login immediately and change your password!"
        )

        # Attempt to create a DM channel and send the success message embed
        try:
             dm_channel = await discord_user.create_dm()
             await dm_channel.send(embed=discord.Embed(
                  # Use Elookup for the title emoji if available, otherwise use a default string
                  title=f"{Elookup('success', '🎉')} Your Rn Nodes Panel Account is Ready!",
                  description=success_msg_dm,
                  color=discord.Color.green()
             ).set_footer(text="Remember to change your password!")) # Add a footer reminder

             # Inform the administrator (the command invoker) that the user was created, linked, and successfully DMed.
             # This follow-up should be ephemeral.
             await send_success_embed(interaction, "User Created & Linked", f"Successfully created panel user for {discord_user.mention}. Details sent via DM.", ephemeral=True)

        except discord.Forbidden:
             # Catch the case where the bot cannot send a DM to the user (e.g., user privacy settings)
             # Inform the administrator non-ephemeral, providing the login details manually.
             await send_success_embed(
                 interaction,
                 "User Created & Linked (DM Failed)",
                 f"Successfully created panel user for {discord_user.mention} and linked, but **could not send DM** (user may have DMs disabled?). Provide details manually:\n"
                 f"Username: `{panel_username}`\n"
                 f"Temp Password: ||`{password}`||", # Include password in spoiler for admin to relay
                 ephemeral=False # Make this message visible to all in the channel (or just the admin if in thread/DM)
             )
        except Exception as e:
             # Catch any other errors during the DM sending process
             print(f"Error sending DM after user creation for user {discord_user.id} ({discord_user.display_name}): {e}") # Log the specific error
             # Inform the administrator non-ephemeral, providing the login details manually.
             await send_success_embed(
                 interaction,
                 "User Created & Linked (DM Error)",
                 f"Successfully created panel user for {discord_user.mention} and linked, but encountered an **error sending DM**. Provide details manually:\n"
                 f"Username: `{panel_username}`\n"
                 f"Temp Password: ||`{password}`||", # Include password in spoiler
                 ephemeral=False # Make this message visible
             )


async def perform_admin_server_action(interaction: discord.Interaction, action: str, http_method: str = 'POST'):
    await interaction.response.defer(ephemeral=True)
    all_servers_data = await make_api_request('GET', '/servers', 'application', interaction, params={'per_page': 100}) # Fetch more for selection
    if not all_servers_data or not isinstance(all_servers_data.get('data'), list):
        if not interaction.response.is_done(): await send_error_embed(interaction, "Fetch Error", "Could not retrieve server list.")
        return
    servers = all_servers_data['data']
    if not servers:
        await send_info_embed(interaction, f"{action.capitalize()} Server", "🍃 No servers found on the panel.", ephemeral=True)
        return

    view = ServerActionView(interaction, servers, action, f"🔎 Select server to {action}...", f"admin_{action}_select")
    await view.start(message_content=f"Please select the server you wish to **{action}**: ")
    await view.wait()

    if view.selected_server_uuid and view.selected_server_data:
        # ServerActionView stores the selected server's full data dict in selected_server_data
        # It should also store the selected value (UUID or ID) in selected_server_uuid
        
        # We need the Application API internal ID for actions like suspend/delete
        server_app_id = view.selected_server_data.get('id') 
        server_name = view.selected_server_data.get('name', view.selected_server_uuid) # Fallback to UUID if name missing

        if not server_app_id:
            await interaction.edit_original_response(content=f"{Elookup('error')} Could not get internal ID for server {server_name}.", view=None, embed=None)
            return
        
        if action == "delete":
            confirm_view = ConfirmView(interaction.user.id)
            await interaction.edit_original_response( # Edit the ServerActionView's message
                content=f"{Elookup('warning')} **Confirm Deletion:** Delete server **{server_name}** (ID: `{server_app_id}`)? **CANNOT BE UNDONE**.",
                embed=None, view=confirm_view
            )
            confirm_view.message = await interaction.original_response()
            await confirm_view.wait()
            if not confirm_view.confirmed: return # User cancelled
            # ConfirmView will edit its own message to "Processing..."
        
        # Use server_app_id for these actions
        endpoint_map = {
            'suspend': f'/servers/{server_app_id}/suspend',
            'unsuspend': f'/servers/{server_app_id}/unsuspend',
            'delete': f'/servers/{server_app_id}' 
        }
        endpoint = endpoint_map.get(action)
        if not endpoint:
            await send_error_embed(interaction, "Bot Error", f"Unknown action '{action}'.")
            return

        response_data = await make_api_request(http_method, endpoint, 'application', interaction) # Pass interaction for error handling
        
        if response_data: # Success (204 or JSON body)
             # The message was likely edited by ConfirmView or is the original selection message
             # Send a new followup for success or edit the last message from ConfirmView
            final_message_target = interaction
            if action == "delete" and confirm_view.message:
                # If delete, the confirm_view message is the most recent one we can edit
                try:
                    await confirm_view.message.edit(content=f"{Elookup('success')} Successfully initiated **{action}** for server **{server_name}** (ID: `{server_app_id}`).", view=None)
                except discord.HTTPException: # Fallback to followup
                    await send_success_embed(final_message_target, f"{Elookup('success')} Server {action.capitalize()}", f"Successfully initiated **{action}** for server **{server_name}** (ID: `{server_app_id}`).", ephemeral=True)
            else: # For suspend/unsuspend or if delete confirm message failed
                await send_success_embed(final_message_target, f"{Elookup('success')} Server {action.capitalize()}", f"Successfully initiated **{action}** for server **{server_name}** (ID: `{server_app_id}`).", ephemeral=True)
        # If response_data is None, make_api_request handled the error message.
    # Timeout or cancellation of ServerActionView handled by the view itself.


@admin_group.command(name="suspend", description=f"{Elookup('power_off')} Suspend a server.")
@is_vps_creator()
async def admin_suspend_server(interaction: discord.Interaction):
    await perform_admin_server_action(interaction, "suspend", http_method='POST')

@admin_group.command(name="unsuspend", description=f"{Elookup('power_on')} Unsuspend a server.")
@is_vps_creator()
async def admin_unsuspend_server(interaction: discord.Interaction):
    await perform_admin_server_action(interaction, "unsuspend", http_method='POST')

@admin_group.command(name="deleteserver", description=f"{Elookup('delete')} Delete a server (Admin).")
@is_vps_creator()
async def admin_delete_server(interaction: discord.Interaction):
    await perform_admin_server_action(interaction, "delete", http_method='DELETE')

# --- Admin Create Server Modal & Command ---
class AdminCreateServerModal(ui.Modal, title='✨ Create New VPS (Admin)'):
    server_name = ui.TextInput(label='Server Name', placeholder='My Awesome Server', required=True, max_length=100)
    hostname_prefix = ui.TextInput(label='Hostname Prefix (Optional)', placeholder='my-server (gets .yourdomain.com)', required=False, max_length=60) # Reduced for FQDN limits
    cpu_cores = ui.TextInput(label='CPU Cores (e.g., 4 for 4 cores)', placeholder='e.g., 4', required=True, max_length=4) # For Convoy Cores
    # Convoy uses CPU Limit (%) e.g. 100 for 1 core, 400 for 4 cores
    memory_mb = ui.TextInput(label='Memory (RAM) in MB', placeholder='e.g., 4096', required=True, max_length=6)
    disk_mb = ui.TextInput(label='Disk Size in MB', placeholder='e.g., 20480', required=True, max_length=7)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            self.cpu_val = int(self.cpu_cores.value) # This is Cores for Convoy, % for Convoy
            self.memory_val = int(self.memory_mb.value)
            self.disk_val = int(self.disk_mb.value)
            if self.cpu_val <= 0 or self.memory_val <= 0 or self.disk_val <= 0: raise ValueError("Resources must be positive.")
        except ValueError:
            await interaction.response.send_message(f"{Elookup('error')} Invalid resource value. Enter positive numbers.", ephemeral=True)
            self.stop()
            return
        await interaction.response.defer(ephemeral=True, thinking=True) # Changed to thinking=True
        self.modal_interaction = interaction # Store the interaction from the modal submit
        self.stop()

    async def on_error(self, interaction: discord.Interaction, error: Exception):
        logger.error(f"Error in AdminCreateServerModal: {error}", exc_info=True)
        if not interaction.response.is_done():
            try: await interaction.response.send_message(f"{Elookup('error')} Oops! Form error. Please try again.", ephemeral=True)
            except discord.HTTPException: pass
        else: # Should not happen if on_submit deferred properly
            try: await interaction.followup.send(f"{Elookup('error')} Oops! Form error. Please try again.", ephemeral=True)
            except discord.HTTPException: pass
        self.stop()

@admin_group.command(name="create", description=f"{Elookup('server', '🛠️')} Guided VPS creation (Admin).")
@is_vps_creator()
@app_commands.describe(assign_to="Optional: Assign to this Discord user.", node_id="Optional: Node ID.", template_uuid="Optional: Template UUID.")
async def admin_create_vps(interaction: discord.Interaction, assign_to: Optional[discord.User] = None, node_id: Optional[int] = None, template_uuid: Optional[str] = None):
    original_cmd_interaction = interaction # This is the /admin create interaction
    target_user_id_panel: Optional[int] = None
    dm_user = original_cmd_interaction.user # Default to self if not assigning

    if assign_to:
        dm_user = assign_to
        linked_id = await get_linked_convoy_id(assign_to.id)
        if not linked_id:
            await send_error_embed(original_cmd_interaction, "User Not Linked", f"{assign_to.mention} needs to `/link` their account first before you can assign a server to them.")
            return
        target_user_id_panel = int(linked_id)
    else: # Admin creating for self
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
            # TODO: Implement node selection dropdown if many nodes and no default/specific ID
            await send_error_embed(original_cmd_interaction, "Node Error", "No Node ID specified and no default node is configured. Please specify a `node_id` or set a default.")
            return

    selected_template_uuid = template_uuid
    if not selected_template_uuid:
        if DEFAULT_TEMPLATE_UUID:
            selected_template_uuid = DEFAULT_TEMPLATE_UUID
            logger.info(f"Using default Template UUID: {selected_template_uuid}")
        else:
            # TODO: Implement template selection dropdown
            await send_error_embed(original_cmd_interaction, "Template Error", "No Template UUID specified and no default template is configured. Please specify a `template_uuid` or set a default.")
            return
    
    modal = AdminCreateServerModal()
    await original_cmd_interaction.response.send_modal(modal)
    await modal.wait() # Wait for modal submission or timeout

    if not hasattr(modal, 'modal_interaction') or modal.modal_interaction is None: # Modal was cancelled or timed out
        # original_cmd_interaction.response is already used by send_modal
        # A followup might be needed if send_modal didn't complete or if user cancelled without submitting.
        # However, if modal.wait() finishes without modal_interaction, it means it wasn't submitted.
        # We can't easily send a "cancelled" message here as the original response is tied to the modal.
        logger.info("Admin VPS creation modal was not submitted or timed out.")
        return

    # Use the modal's interaction for followups related to processing its data
    # The original_cmd_interaction is for logging who initiated the command
    modal_interaction = modal.modal_interaction 
    # modal_interaction.response is already deferred and set to thinking by on_submit

    temp_password = generate_compliant_password(length=16)
    
    # Hostname generation
    hostname_prefix_val = modal.hostname_prefix.value.strip().lower().replace(' ', '-') if modal.hostname_prefix.value else modal.server_name.value.strip().lower().replace(' ', '-')
    # Basic sanitization for hostname prefix
    hostname_prefix_val = "".join(c for c in hostname_prefix_val if c.isalnum() or c == '-')[:60]
    full_hostname = f"{hostname_prefix_val}.{DEFAULT_SERVER_HOSTNAME_SUFFIX}"

    assigned_ip_from_file = get_and_remove_first_ip() 
    # Note: This IP string needs to be mapped to an `address_id` (allocation ID) for Convoy v4 API.
    # This mapping is complex and usually requires listing available allocations on the node.
    # For now, we'll log the IP and the admin needs to ensure the panel handles it or manually map it.
    
    next_vmid = random.randint(200, 9999) # For Convoy v4 VMID
    logger.info(f"Generated VMID for admin VPS creation: {next_vmid}")

    # Payload for Convoy v4 API
    payload_v4 = {
        "node_id": selected_node_id,
        "user_id": target_user_id_panel,
        "name": modal.server_name.value.strip(),
        "hostname": full_hostname,
        "vmid": next_vmid, 
        "limits": {
            "cpu": modal.cpu_val, # Number of cores (as per modal label)
            "memory": modal.memory_val * 1024 * 1024, # MB to Bytes
            "disk": modal.disk_val * 1024 * 1024,     # MB to Bytes
            "snapshots": DEFAULT_ADMIN_SNAPSHOT_LIMIT,
            "backups": DEFAULT_ADMIN_BACKUP_LIMIT,
            "bandwidth": 0, # Unlimited (or a configured default)
            "address_ids": [] # Placeholder: Needs IDs of allocations
        },
        "feature_limits": { # These are Convoy-style feature limits
             "allocations": config.get('defaults',{}).get('admin_allocation_limit',1), 
             "databases": config.get('defaults',{}).get('admin_database_limit',0),
             "backups": config.get('defaults',{}).get('admin_backup_limit_total', 5) # Max storable
        },
        "account_password": temp_password, 
        "template_uuid": selected_template_uuid,
        "should_create_server": True, # For Convoy v4, tells panel to provision
        "start_on_completion": True,
    }

    if assigned_ip_from_file:
        logger.info(f"Admin VPS creation requested with specific IP '{assigned_ip_from_file}'. This IP needs to be mapped to an 'address_id' on node {selected_node_id} and added to 'payload_v4[\"limits\"][\"address_ids\"]'. Panel might auto-assign if not explicitly set.")
        # Example: If you had a function get_address_id_from_ip(ip_str, node_id), you'd call it here.
        # For now, 'address_ids' remains empty, panel will pick or use default.
        
    logger.info(f"Admin VPS creation payload (Convoy v4 style): {json.dumps(payload_v4, indent=2)}")
    # Use modal_interaction for the API call as its response is the one we're managing
    creation_response = await make_api_request('POST', '/servers', 'application', modal_interaction, json_data=payload_v4)

    if creation_response and isinstance(creation_response.get('data'), dict):
        created_data = creation_response['data']
        server_name_resp = created_data.get('name', modal.server_name.value)
        server_short_id = created_data.get('id', 'N/A') # App API short ID
        server_uuid_pterodactyl = created_data.get('uuid', 'N/A')   # Convoy/Convoy internal UUID
        
        primary_ip_address = "Panel Assigned" # Default
        # Convoy v4 response structure for addresses might be under 'addresses' list in 'data'
        if created_data.get('addresses') and isinstance(created_data['addresses'], list) and created_data['addresses']:
            primary_ip_address = created_data['addresses'][0].get('address', 'Panel Assigned (Error Reading)')
        elif assigned_ip_from_file: # If we requested one and panel didn't return it explicitly
            primary_ip_address = f"{assigned_ip_from_file} (Requested, Verify Assignment)"

        details_embed = discord.Embed(
            title=f"{Elookup('success')} Admin VPS Created: {server_name_resp}",
            description=f"A new VPS has been successfully provisioned for {dm_user.mention}!", color=discord.Color.brand_green()
        )
        details_embed.add_field(name="🔗 Panel Link", value=f"<{PANEL_BASE_URL}/server/{server_uuid_pterodactyl if server_uuid_pterodactyl != 'N/A' else server_short_id}>", inline=False)
        details_embed.add_field(name="🏷️ Server Name", value=f"`{server_name_resp}`", inline=True)
        details_embed.add_field(name="🆔 Server ID (App)", value=f"`{server_short_id}`", inline=True)
        details_embed.add_field(name="🔑 Server UUID (Convoy)", value=f"`{server_uuid_pterodactyl}`", inline=True)
        details_embed.add_field(name="🌐 Hostname", value=f"`{created_data.get('hostname', full_hostname)}`", inline=True)
        details_embed.add_field(name="🔌 Primary IP", value=f"`{primary_ip_address}`", inline=True)
        
        resp_limits = created_data.get('limits', {})
        cpu_resp = resp_limits.get('cpu', payload_v4['limits']['cpu']) # Cores
        mem_resp_bytes = resp_limits.get('memory', payload_v4['limits']['memory'])
        disk_resp_bytes = resp_limits.get('disk', payload_v4['limits']['disk'])
        mem_gb = mem_resp_bytes / (1024**3) if mem_resp_bytes else modal.memory_val / 1024
        disk_gb = disk_resp_bytes / (1024**3) if disk_resp_bytes else modal.disk_val / 1024

        details_embed.add_field(name=f"{Elookup('cpu')} CPU Cores", value=f"`{cpu_resp}`", inline=True)
        details_embed.add_field(name=f"{Elookup('ram')} Memory", value=f"`{mem_gb:.1f} GB`", inline=True)
        details_embed.add_field(name=f"{Elookup('disk')} Disk", value=f"`{disk_gb:.1f} GB`", inline=True)
        details_embed.add_field(name=f"{Elookup('password')} Initial Root Password", value=f"||`{temp_password}`|| (Login and change immediately!)", inline=False)
        details_embed.set_footer(text=f"VMID: {created_data.get('vmid','N/A')} | Node: {created_data.get('node_id','N/A')} | Template: {created_data.get('template_uuid', selected_template_uuid)[:10]}...")

        dm_sent_successfully = False
        try:
            dm_channel = await dm_user.create_dm()
            await dm_channel.send(embed=details_embed)
            dm_sent_successfully = True
        except discord.Forbidden:
            # Send details publicly if DM fails, using modal_interaction's followup
            await modal_interaction.followup.send(
                content=f"{Elookup('success')} VPS **{server_name_resp}** created for {dm_user.mention}, but **could not send DM with details.** Details below:",
                embed=details_embed, ephemeral=False # Make it non-ephemeral so admin/user can see
            )
        except Exception as e:
            logger.error(f"Error sending DM for admin-created VPS: {e}")
            await modal_interaction.followup.send(
                content=f"{Elookup('error')} VPS **{server_name_resp}** created for {dm_user.mention}, but **error sending DM.** Details below:",
                embed=details_embed, ephemeral=False
            )

        if dm_sent_successfully:
            await send_success_embed(modal_interaction, "VPS Created!", f"Successfully started VPS **{server_name_resp}** (ID: `{server_short_id}`). Details have been DM'd to {dm_user.mention}.", ephemeral=True)
        
        # Log using original_cmd_interaction.user as the initiator
        await send_vps_log("Admin Creation", original_cmd_interaction.user, details_embed, server_uuid_pterodactyl, server_short_id, server_name_resp)
    
    # If creation_response is None or not the expected dict, make_api_request would have sent an error message
    # using modal_interaction if its response wasn't already done. Since on_submit deferred with thinking=True,
    # make_api_request *should* be able to send errors via modal_interaction.followup.


# --- User VPS Creation (/create) ---
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
        if BOOST_REWARDS_ENABLED and BOOST_REWARD_TIERS: # Check if tiers exist
            for i, tier in enumerate(BOOST_REWARD_TIERS):
                boost_options.append(discord.SelectOption(
                    label=f"{Elookup('boost_plan','🚀')} {tier.get('name', f'Boost Tier {i+1}')} ({tier.get('server_boosts_required','N/A')} Boosts)",
                    value=f"boost_{i}",
                    description=f"{tier.get('ram_gb','N/A')}GB RAM, {tier.get('cpu_cores','N/A')} CPU, {tier.get('disk_gb','N/A')}GB Disk"[:100]
                ))
        if boost_options:
            self.add_item(discord.ui.Select(placeholder=f"{Elookup('boost_plan','🚀')} Select Boost Plan (If Boosting)", options=boost_options[:25], custom_id="select_boost_plan"))
        else: 
            self.add_item(discord.ui.Select(placeholder=f"{Elookup('boost_plan','🚀')} Boost Rewards Unavailable", options=[discord.SelectOption(label="N/A", value="na_boost")], disabled=True, custom_id="select_boost_plan_disabled"))

        invite_options = []
        if INVITE_REWARDS_ENABLED_GLOBAL and INVITE_REWARD_TIERS: # Check if tiers exist
            for i, tier in enumerate(INVITE_REWARD_TIERS):
                invite_options.append(discord.SelectOption(
                    label=f"{Elookup('invite_plan','💌')} {tier.get('name', f'Invite Tier {i+1}')} ({tier.get('invites_required','N/A')} Invites)",
                    value=f"invite_{i}",
                    description=f"{tier.get('ram_gb','N/A')}GB RAM, {tier.get('cpu_cores','N/A')} CPU, {tier.get('disk_gb','N/A')}GB Disk"[:100]
                ))
        if invite_options:
            self.add_item(discord.ui.Select(placeholder=f"{Elookup('invite_plan','💌')} Select Invite Plan (If Qualified)", options=invite_options[:25], custom_id="select_invite_plan"))
        else:
            self.add_item(discord.ui.Select(placeholder=f"{Elookup('invite_plan','💌')} Invite Rewards Unavailable", options=[discord.SelectOption(label="N/A", value="na_invite")], disabled=True, custom_id="select_invite_plan_disabled"))

        if PAID_PLANS_DATA: # Only show if paid plans are configured
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
        if "_" not in selected_value or selected_value.startswith("na_"): # Handle "na_boost" or "na_invite"
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

        await interaction.response.defer() # Acknowledge the selection interaction
        if self.message:
            try:
                selected_label = self.selected_plan_data.get('name', f"{plan_category.title()} Plan")
                await self.message.edit(content=f"{Elookup('confirm','✅')} Plan selected: **{selected_label}**. Please wait, processing...", view=None, embed=None)
            except discord.HTTPException as e:
                 logger.error(f"PlanSelectView: Error editing message on selection: {e}")
        self.stop()

    async def button_callback(self, interaction: discord.Interaction): # For "Paid Plan Request"
        self.selected_plan_type = "paid"
        self.selected_plan_data = {"name": "Paid Plan Request"} 
        await interaction.response.defer()
        if self.message:
            try: await self.message.edit(content=f"{Elookup('confirm','✅')} Selection: **Paid Plan Request**. Proceeding...", view=None, embed=None)
            except discord.HTTPException as e:
                logger.error(f"PlanSelectView: Error editing message on button press: {e}")
        self.stop()
    
    async def on_timeout(self):
        if self.message and self.selected_plan_type is None: # Only edit if no selection was made
            try:
                for item in self.children: item.disabled = True
                await self.message.edit(content=f"{Elookup('loading')} Plan selection timed out. Use `/create` again if you wish to proceed.", view=self, embed=None)
            except discord.HTTPException as e:
                logger.warning(f"PlanSelectView: Error editing message on timeout: {e}")


class AdminConfirmationView(discord.ui.View): 
    def __init__(self, requesting_user_id: int, plan_name: str, server_payload: dict, temp_password: str, assigned_ip_ref: str, timeout=7200): # 2 hours
        super().__init__(timeout=timeout)
        self.requesting_user_id = requesting_user_id
        self.plan_name = plan_name
        self.server_payload = server_payload 
        self.temp_password = temp_password
        self.assigned_ip_ref = assigned_ip_ref 
        self.status: Optional[bool] = None # True for approved, False for denied
        self.response_admin: Optional[discord.User] = None
        self.message: Optional[discord.Message] = None # The message this view is attached to
        
    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if not interaction.guild or not isinstance(interaction.user, discord.Member) or not check_is_vps_creator(interaction):
            await interaction.response.send_message(f"{Elookup('error')} Only VPS Creators can approve/deny these requests.", ephemeral=True)
            return False
        return True

    async def _update_view_on_action(self, interaction: discord.Interaction, approved: bool):
        self.status = approved
        self.response_admin = interaction.user
        
        # Disable all buttons
        for item in self.children:
            if isinstance(item, discord.ui.Button): item.disabled = True

        if self.message and self.message.embeds:
            embed = self.message.embeds[0].copy() # Get the first embed
            action_taken_str = "Approved" if approved else "Denied"
            action_color = discord.Color.green() if approved else discord.Color.red()
            
            embed.title = f"{Elookup('info')} VPS Request {action_taken_str}"
            embed.color = action_color
            # Clear existing fields that might show temp password or mutable details if any
            # embed.clear_fields() # Or selectively remove/update fields
            embed.add_field(name=f"{action_taken_str} By", value=f"{interaction.user.mention} at {discord.utils.format_dt(datetime.now(timezone.utc))}", inline=False)
            
            await interaction.response.edit_message(embed=embed, view=self) # Update the message with new embed and disabled buttons
        else: # Fallback if no embed or message
            await interaction.response.edit_message(content=f"Request {action_taken_str.lower()} by {interaction.user.mention}. Buttons disabled.", view=self)
        self.stop()

    @discord.ui.button(label="Approve Request", style=discord.ButtonStyle.green, custom_id="vps_req_approve_btn")
    async def approve_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._update_view_on_action(interaction, True)
    
    @discord.ui.button(label="Deny Request", style=discord.ButtonStyle.red, custom_id="vps_req_deny_btn")
    async def deny_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._update_view_on_action(interaction, False)
    
    async def on_timeout(self):
        if self.status is None and self.message: # If no action was taken
            embed_title = f"{Elookup('loading')} VPS Request Timed Out"
            embed_desc = "This request expired due to no administrator response within the allocated time."
            
            new_embed: Optional[discord.Embed] = None
            if self.message.embeds:
                embed = self.message.embeds[0].copy()
                embed.title = embed_title
                embed.color = discord.Color.dark_gray()
                # Add a field indicating timeout, rather than clearing all
                embed.add_field(name="Status Update", value=embed_desc, inline=False)
                new_embed = embed
            else: # Fallback if original message had no embed
                new_embed = discord.Embed(title=embed_title, description=embed_desc, color=discord.Color.dark_gray())

            for item in self.children: 
                if isinstance(item, discord.ui.Button): item.disabled = True
            try: 
                await self.message.edit(embed=new_embed, view=self)
            except discord.HTTPException as e: 
                logger.error(f"Error updating admin confirmation view on timeout: {e}")

@bot.tree.command(name="create", description=f"{Elookup('server')} Create a VPS based on available plans.")
async def create_vps(interaction: discord.Interaction):
    # PERBAIKAN: Hapus pengecekan yang salah yang memblokir user biasa
    await interaction.response.defer(ephemeral=True) # Initial defer for the command
    user_panel_id = await get_linked_convoy_id(interaction.user.id)
    if not user_panel_id:
        await send_error_embed(interaction, "Account Not Linked", "Please use `/link` to link your Discord account to your panel account before creating a VPS.")
        return

    view = PlanSelectView(interaction.user.id, interaction)
    # Send an ephemeral message for plan selection
    msg_obj = await send_info_embed(
        interaction, "VPS Creation: Select Your Plan", "Choose your desired VPS plan from the options below:", view=view, ephemeral=True
    )
    if not msg_obj:
        logger.error(f"Failed to get message object for PlanSelectView for user {interaction.user.id}. `send_info_embed` returned None.")
        # send_info_embed would have tried to send an error, but if it failed completely:
        await interaction.followup.send(f"{Elookup('error')} An error occurred trying to display plan selection. Please try again.", ephemeral=True)
        return
    view.message = msg_obj # Store the WebhookMessage for editing by the view
    await view.wait() # Wait for user to select a plan or timeout

    if view.selected_plan_type is None or view.selected_plan_data is None:
        # Timeout or cancellation is handled by the view's on_timeout or if it stops early
        # If it was explicitly cancelled by user action in the view (not currently implemented, but good practice)
        if not view.is_finished() and view.selected_plan_type is None:
             await interaction.followup.send(f"{Elookup('info')} Plan selection was cancelled or no valid plan was chosen.", ephemeral=True)
        # If timed out, the view itself edits its message.
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
            try: await owner.send(f"🔔 User {interaction.user.mention} ({interaction.user.id}) has initiated a **paid VPS plan request** through the bot.")
            except discord.HTTPException as e: logger.warning(f"Could not DM Bot Owner about paid plan request: {e}")
        return

    # --- Common setup for Boost/Invite plans ---
    # Ensure plan_data has the necessary keys (cpu_cores, ram_gb, disk_gb)
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

    # Verify requirements (Boost/Invite)
    if plan_type == "boost":
        if not BOOST_REWARDS_ENABLED:
            await send_error_embed(interaction, "Boost Rewards Disabled", "Server Boost rewards are currently disabled by the administrators.")
            return # IP should be re-added to file if not used
        if not interaction.guild: 
            await send_error_embed(interaction, "Context Error", "Cannot verify boost status outside a server environment.")
            return
        
        member = interaction.guild.get_member(interaction.user.id)
        if not member: # Should not happen with slash command in guild
            try: member = await interaction.guild.fetch_member(interaction.user.id)
            except discord.NotFound:
                 await send_error_embed(interaction, "Verification Error", "Could not find your member information in this server.")
                 return
        
        boost_req = plan_data.get('server_boosts_required', 999) # A high number if not set
        if not member.premium_since: # Check if user is boosting THIS server
            await send_error_embed(interaction, "Boost Verification Failed", f"You are not currently boosting **{interaction.guild.name}**. This plan requires you to be an active booster of this server.")
            return
        # Note: Checking guild.premium_subscription_count might not be what you want for "2x Boost plan"
        # "2x Boost" usually means the user provides 2 boosts. Discord doesn't easily show *how many* boosts one user provides.
        # The config 'server_boosts_required' might mean "the server must have at least X total boosts".
        # If it means the *user* must provide X boosts, that's harder to verify directly.
        # Assuming 'server_boosts_required' is about the user actively boosting this specific server (premium_since).
        # And perhaps a global server boost level check:
        if interaction.guild.premium_tier < plan_data.get('minimum_server_tier_required', 0): # Example: Tier 1 = 2 boosts, Tier 2 = 7, Tier 3 = 14
             await send_error_embed(interaction, "Server Boost Level Too Low", f"This server's boost level (Tier {interaction.guild.premium_tier}) is not high enough for this specific reward plan. It requires the server to be at least Tier {plan_data.get('minimum_server_tier_required',0)}.")
             return


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

    # --- Construct Payload & Send for Admin Approval ---
    safe_user_name = "".join(filter(str.isalnum, interaction.user.name))[:10] or "user"
    server_name_gen = f"{plan_type[:3].upper()}-{safe_user_name}-{random.randint(100,999)}"[:50]
    hostname_gen = f"{server_name_gen.lower().replace(' ', '-')}.{DEFAULT_SERVER_HOSTNAME_SUFFIX}"
    
    next_vmid = random.randint(200, 9999) 
    logger.info(f"Generated VMID for user plan creation ({plan_name_log}): {next_vmid}")
    
    # Payload for Convoy v4 API - Ensure cpu_cores is used from plan_data
    server_creation_payload = {
        "node_id": target_node_id,
        "user_id": int(user_panel_id),
        "name": server_name_gen,
        "hostname": hostname_gen,
        "vmid": next_vmid,
        "limits": {
            "cpu": plan_data['cpu_cores'], # Using cpu_cores from plan_data
            "memory": plan_data['ram_gb'] * 1024 * 1024 * 1024, # GB to Bytes
            "disk": plan_data['disk_gb'] * 1024 * 1024 * 1024,   # GB to Bytes
            "snapshots": plan_data.get('snapshot_limit', DEFAULT_USER_SNAPSHOT_LIMIT),
            "backups": plan_data.get('backup_limit', DEFAULT_USER_BACKUP_LIMIT),
            "bandwidth": 0, 
            "address_ids": [] # IP will be auto-assigned by panel or admin maps assigned_ip_from_file
        },
        "feature_limits": { # Convoy-style feature limits
            "allocations": plan_data.get('allocation_limit', 1),
            "databases": plan_data.get('database_limit', 0),
            "backups": plan_data.get('total_backup_limit', config.get('defaults',{}).get('user_total_backup_limit',3)) # Max storable
        },
        "account_password": temp_password, 
        "template_uuid": selected_template_uuid,
        "should_create_server": True, # For Convoy v4 to provision
        "start_on_completion": True,
    }
    # Log the IP that was reserved from file for this request
    logger.info(f"IP '{assigned_ip_from_file}' from {IP_FILENAME} has been reserved for user {interaction.user.id}'s potential VPS ({plan_name_log}). It will be used if admin approves.")

    await send_info_embed(interaction, f"{Elookup('loading')} Request Submitted for Approval",
                          f"Your request for the **{plan_name_log}** VPS is now awaiting administrator approval.\n"
                          f"You will receive a DM once it has been reviewed. The IP address ` {assigned_ip_from_file} ` has been tentatively reserved for you if this request is approved.",
                          ephemeral=True)

    if not ADMIN_VPS_APPROVAL_CHANNEL_ID:
        logger.error("`admin_vps_approval_channel_id` is not set in config.json. Cannot send VPS creation request for admin approval.")
        await interaction.followup.send(f"{Elookup('error')} An internal configuration error occurred (admin approval channel not set). Please notify an administrator.", ephemeral=True)
        # Return the IP to the file if approval cannot proceed
        # This is a simplified re-add; a robust solution might use a temporary holding list.
        try:
            with open(IP_FILENAME, 'a') as f: f.write(f"\n{assigned_ip_from_file}\n")
            logger.info(f"Returned IP '{assigned_ip_from_file}' to {IP_FILENAME} due to approval channel error.")
        except IOError: logger.error(f"Failed to return IP '{assigned_ip_from_file}' to {IP_FILENAME}.")
        return
        
    admin_channel = bot.get_channel(ADMIN_VPS_APPROVAL_CHANNEL_ID)
    if not admin_channel or not isinstance(admin_channel, discord.TextChannel):
        logger.error(f"Admin approval channel ID {ADMIN_VPS_APPROVAL_CHANNEL_ID} is invalid or bot cannot access it.")
        await interaction.followup.send(f"{Elookup('error')} An internal error occurred (admin approval channel invalid). Please notify an administrator.", ephemeral=True)
        try:
            with open(IP_FILENAME, 'a') as f: f.write(f"\n{assigned_ip_from_file}\n")
            logger.info(f"Returned IP '{assigned_ip_from_file}' to {IP_FILENAME} due to invalid approval channel.")
        except IOError: logger.error(f"Failed to return IP '{assigned_ip_from_file}' to {IP_FILENAME}.")
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
    approval_embed.add_field(name="🔌 Reserved IP (from file)", 
                              value=f"`{assigned_ip_from_file}`\n"
                                    f"**Admin Action:** If approving, ensure this IP is mapped to a free allocation ID on node `{target_node_id}`. This ID should be added to the server's `address_ids` list in the panel *after creation if not auto-assigned*, or modify payload before creation if manual pre-assignment is possible.", 
                              inline=False)
    
    # Displaying specs using cpu_cores from plan_data
    approval_embed.add_field(name="⚙️ Plan Specifications", 
                              value=f"{Elookup('cpu','⚙️')} CPU Cores: **{plan_data['cpu_cores']}**\n"
                                    f"{Elookup('ram','💾')} RAM: **{plan_data['ram_gb']}GB**\n"
                                    f"{Elookup('disk','📀')} Disk: **{plan_data['disk_gb']}GB SSD**", 
                              inline=False)
    approval_embed.add_field(name=f"{Elookup('password')} Temporary Root Password", value=f"||`{temp_password}`|| (User will be DMed this)", inline=False)
    approval_embed.set_footer(text=f"Target Node: {target_node_id} | Template UUID: {selected_template_uuid[:12]}... | Proposed VMID: {next_vmid}")

    admin_conf_view = AdminConfirmationView(interaction.user.id, plan_name_log, server_creation_payload, temp_password, assigned_ip_from_file)
    
    try:
        ping_role = interaction.guild.get_role(VPS_CREATOR_ROLE_ID) if interaction.guild else None
        content_msg = f"{ping_role.mention if ping_role else '@VPS Creators'} New VPS creation request requires your approval:"
        
        approval_msg_obj = await admin_channel.send(content=content_msg, embed=approval_embed, view=admin_conf_view)
        admin_conf_view.message = approval_msg_obj # Store the message for the view to update
    except discord.HTTPException as e:
        logger.error(f"Failed to send VPS approval request to admin channel: {e}")
        await interaction.followup.send(f"{Elookup('error')} Failed to submit your request to the administrators due to a Discord error. Please try again later or contact support.", ephemeral=True)
        # Return IP
        try:
            with open(IP_FILENAME, 'a') as f: f.write(f"\n{assigned_ip_from_file}\n")
            logger.info(f"Returned IP '{assigned_ip_from_file}' to {IP_FILENAME} due to admin channel send error.")
        except IOError: logger.error(f"Failed to return IP '{assigned_ip_from_file}' to {IP_FILENAME}.")
        return

    await admin_conf_view.wait() # Wait for admin to approve/deny or timeout

    # --- Post-Approval Processing ---
    if admin_conf_view.status is True: # Approved by admin
        final_payload_to_api = admin_conf_view.server_payload # Get payload from the view
        
        # Admin should have manually ensured the assigned_ip_from_file is mapped to an address_id if needed.
        # If the panel auto-assigns from a pool when address_ids is empty, this is fine.
        # If specific IP assignment is critical AND panel requires address_id in payload:
        # An admin would need to:
        # 1. Find the allocation ID for assigned_ip_from_file on target_node_id.
        # 2. Edit the server_creation_payload (e.g., via an admin command before final creation)
        #    or the bot would need a pre-approval step for admins to input this ID.
        # For now, we assume panel handles it or admin does post-creation config if IP doesn't match.
        logger.info(f"Admin approved. Final payload for user VPS ({plan_name_log}): {json.dumps(final_payload_to_api, indent=2)}")
        
        # Use the original command's interaction for the API call, as the admin_conf_view's interaction is ephemeral and done.
        creation_response = await make_api_request('POST', '/servers', 'application', interaction, json_data=final_payload_to_api)

        if creation_response and isinstance(creation_response.get('data'), dict):
            created_data = creation_response['data']
            final_server_name = created_data.get('name', server_name_gen)
            server_short_id = created_data.get('id', 'N/A') # App API ID
            server_uuid_pterodactyl = created_data.get('uuid', 'N/A') # Convoy UUID
            vmid_resp = created_data.get('vmid', next_vmid) # VMID from response or our generated one

            if plan_type == "invite" and interaction.guild: # Reset invites if it was an invite plan
                if reset_user_invites(interaction.guild.id, interaction.user.id):
                    logger.info(f"Reset invites for user {interaction.user.id} in guild {interaction.guild.id} after VPS creation.")
                else:
                    logger.warning(f"Failed to reset invites for user {interaction.user.id} after VPS creation.")

            final_assigned_ip_str = "Panel Assigned (Verify in Panel)"
            if created_data.get('addresses') and isinstance(created_data['addresses'], list) and created_data['addresses']:
                final_assigned_ip_str = created_data['addresses'][0].get('address', 'Error Reading IP')
            elif assigned_ip_from_file: # If panel didn't explicitly return it, refer to the reserved one
                final_assigned_ip_str = f"{assigned_ip_from_file} (Reserved, Verify)"
            
            details_desc = (
                f"🎉 Congratulations! Your **{plan_name_log}** VPS request was approved by {admin_conf_view.response_admin.mention} and your server is now ready!\n\n"
                f"**🔗 Panel Link:** <{PANEL_BASE_URL}/server/{server_uuid_pterodactyl if server_uuid_pterodactyl != 'N/A' else server_short_id}>\n"
                f"**🏷️ Server Name:** `{final_server_name}` (App ID: `{server_short_id}`)\n"
                f"**🔌 Assigned IP Address:** `{final_assigned_ip_str}`\n"
                f"**🌐 Hostname:** `{created_data.get('hostname', hostname_gen)}`\n\n"
                f"⚙️ **Server Resources:**\n"
                f"   {Elookup('cpu','⚙️')} CPU Cores: **{plan_data['cpu_cores']}**\n" # Using cpu_cores
                f"   {Elookup('ram','💾')} RAM: **{plan_data['ram_gb']}GB**\n"
                f"   {Elookup('disk','📀')} Disk: **{plan_data['disk_gb']}GB SSD**\n\n"
                f"🔑 **Initial Root/Admin Password:** ||`{admin_conf_view.temp_password}`||\n\n"
                f"⚠️ **IMPORTANT:** Please log into your server via the panel or SSH as soon as possible and **change this temporary password.** You may also need to configure your server (e.g., firewall, applications) after initial login. The assigned IP might take a few moments to fully propagate."
            )
            dm_embed = discord.Embed(title=f"{Elookup('success')} Your Rn Nodes VPS is Ready!", description=details_desc, color=discord.Color.brand_green())
            dm_embed.set_footer(text=f"Server UUID (Convoy): {server_uuid_pterodactyl} | VMID: {vmid_resp} | Node: {created_data.get('node_id', target_node_id)}")

            dm_sent_successfully = False
            try:
                await interaction.user.send(embed=dm_embed)
                dm_sent_successfully = True
            except discord.HTTPException:
                # If DM fails, send details as a public followup to the original /create command
                await interaction.followup.send(
                    f"{Elookup('success')} VPS **{final_server_name}** created for you! However, I couldn't DM you the details. **Please see them below and save them securely:**", 
                    embed=dm_embed, ephemeral=False # Non-ephemeral so user can see it
                )
            
            if dm_sent_successfully:
                await send_success_embed(interaction, "VPS Approved & Created!", f"Your **{plan_name_log}** VPS request was approved and the server is now ready! Please check your DMs for the server details.", ephemeral=True)
            
            # Log the successful creation
            await send_vps_log(f"User Approved Creation ({plan_name_log})", interaction.user, dm_embed, server_uuid_pterodactyl, server_short_id, final_server_name)
        
        else: # API creation FAILED post-approval
            await send_error_embed(interaction, "VPS Creation Failed Post-Approval", "Your VPS request was approved by an admin, but an error occurred during the final server creation process on the panel. Please contact an administrator for assistance. The reserved IP was not used.")
            if admin_conf_view.response_admin:
                try: await admin_conf_view.response_admin.send(f"⚠️ **CRITICAL FAILURE:** VPS creation FAILED for user {interaction.user.mention} (Plan: {plan_name_log}) **after your approval**. Please investigate the panel logs. The IP `{assigned_ip_from_file}` should be manually returned to `{IP_FILENAME}`.")
                except discord.HTTPException: pass
            # IP should be returned to file if creation fails
            try:
                with open(IP_FILENAME, 'a') as f: f.write(f"\n{assigned_ip_from_file}\n")
                logger.info(f"Returned IP '{assigned_ip_from_file}' to {IP_FILENAME} due to post-approval creation failure.")
            except IOError: logger.error(f"Failed to return IP '{assigned_ip_from_file}' to {IP_FILENAME} after creation failure.")

    elif admin_conf_view.status is False: # Denied by admin
        try:
            await interaction.user.send(embed=discord.Embed(
                title=f"{Elookup('error')} VPS Request Denied",
                description=f"Unfortunately, your request for the **{plan_name_log}** VPS was denied by administrator {admin_conf_view.response_admin.mention}. If you have questions, please open a support ticket.",
                color=discord.Color.red()
            ))
        except discord.HTTPException: # DM failed
            await interaction.followup.send("Your VPS creation request was denied by an administrator. The reserved IP was not used.", ephemeral=True)
        # Return IP to file
        try:
            with open(IP_FILENAME, 'a') as f: f.write(f"\n{assigned_ip_from_file}\n")
            logger.info(f"Returned IP '{assigned_ip_from_file}' to {IP_FILENAME} as request was denied.")
        except IOError: logger.error(f"Failed to return IP '{assigned_ip_from_file}' to {IP_FILENAME} after denial.")
    
    else: # AdminConfirmationView timed out
        try:
            await interaction.user.send(embed=discord.Embed(
                title=f"{Elookup('loading')} VPS Request Timed Out",
                description=f"Your request for the **{plan_name_log}** VPS has timed out as no administrator action was taken within the allowed time. You can try submitting the request again later. The reserved IP was not used.",
                color=discord.Color.dark_gray()
            ))
        except discord.HTTPException: # DM failed
            await interaction.followup.send("Your VPS creation request timed out due to no administrator action. The reserved IP was not used.", ephemeral=True)
        # Return IP to file
        try:
            with open(IP_FILENAME, 'a') as f: f.write(f"\n{assigned_ip_from_file}\n")
            logger.info(f"Returned IP '{assigned_ip_from_file}' to {IP_FILENAME} as request timed out.")
        except IOError: logger.error(f"Failed to return IP '{assigned_ip_from_file}' to {IP_FILENAME} after timeout.")


# --- Toggle Commands (Bot Owner) ---
@bot.tree.command(name="toggleboostrewards", description=f"{Elookup('owner')} Enable/disable server boost rewards.")
@is_bot_owner()
@app_commands.describe(enable="Set to True to enable, False to disable.")
async def toggle_boost_rewards(interaction: discord.Interaction, enable: bool):
    global BOOST_REWARDS_ENABLED, config
    if 'reward_plans_enabled' not in config: config['reward_plans_enabled'] = {}
    config['reward_plans_enabled']['boost'] = enable
    
    if save_json_file(CONFIG_FILE, config):
        BOOST_REWARDS_ENABLED = enable 
        status_msg = "ENABLED 🟢" if enable else "DISABLED 🔴"
        await send_success_embed(interaction, "Boost Rewards Setting Updated", f"Server Boost rewards have been **{status_msg}**.", ephemeral=True)
    else:
        await send_error_embed(interaction, "Configuration Save Error", "Failed to update the configuration file. The change may not persist after a bot restart.")

@bot.tree.command(name="toggleinviterewards", description=f"{Elookup('owner')} Enable/disable invite rewards globally.")
@is_bot_owner()
@app_commands.describe(enable="Set to True to enable, False to disable.")
async def toggle_invite_rewards(interaction: discord.Interaction, enable: bool):
    global INVITE_REWARDS_ENABLED_GLOBAL, config 
    if 'reward_plans_enabled' not in config: config['reward_plans_enabled'] = {}
    config['reward_plans_enabled']['invite'] = enable
    
    if save_json_file(CONFIG_FILE, config):
        INVITE_REWARDS_ENABLED_GLOBAL = enable 
        status_msg = "ENABLED 🟢" if enable else "DISABLED 🔴"
        await send_success_embed(interaction, "Invite Rewards Setting Updated", f"Global invite-based rewards have been **{status_msg}**.", ephemeral=True)
    else:
        await send_error_embed(interaction, "Configuration Save Error", "Failed to update the configuration file. The change may not persist after a bot restart.")


# --- Helper for user server actions (start, stop, etc.) ---
async def _execute_server_power_action(interaction: discord.Interaction, server_uuid: str, server_name: str, action: str):
    """Sends a power signal to the client API."""
    signal_map = {
        "start": "start", "stop": "stop",
        "restart": "restart", "kill": "kill"
    }
    if action not in signal_map:
        await send_error_embed(interaction, "Invalid Action", f"Unknown power action: {action}")
        return

    # Use interaction.followup as the original interaction (from _handle_user_server_action_selection)
    # or the button interaction (from ManageServerView) would have already been responded to (deferred).
    processing_message = await interaction.followup.send(f"{Elookup('loading')} Attempting to **{action}** server **{server_name}** (UUID: `{server_uuid[:8]}`)...", ephemeral=True)
    
    payload = {"signal": signal_map[action]}
    # For power actions, we need the Client API and the Convoy UUID (server_uuid)
    response = await make_api_request('POST', f'/servers/{server_uuid}/power', 'client', interaction, json_data=payload)
    
    if response is not None: 
        if response.get("status_code") == 204 or (isinstance(response, dict) and not response.get('errors')):
            # Edit the "Attempting to..." message
            await processing_message.edit(content=f"{Elookup('success')} Successfully sent **{action}** signal to server **{server_name}** (`{server_uuid[:8]}`).")
        elif isinstance(response, dict) and response.get('errors'):
             # make_api_request should have handled this if interaction wasn't done.
             # If it was done (e.g. by defer), we need to edit our processing_message
            error_detail = response['errors'][0].get('detail', 'Unknown API error during power action.')
            await processing_message.edit(content=f"{Elookup('error')} Failed to {action} server **{server_name}**: {error_detail}")
    else: # response is None, make_api_request handled it or network error
        # If processing_message was sent, we might want to update it indicating failure if make_api_request didn't.
        # However, make_api_request tries to send its own error if interaction is available.
        # For robustness, if processing_message exists and no specific error was sent by make_api_request:
        if processing_message:
            try:
                # Check if an error was already sent by make_api_request via followup
                # This is hard to check directly, so we assume if make_api_request failed, it sent something.
                # If it failed silently (e.g. interaction already responded and followup failed), this provides fallback.
                if not interaction.response.is_done(): # Should not happen if we used followup for processing_message
                    await processing_message.edit(content=f"{Elookup('error')} An unspecified error occurred trying to {action} server **{server_name}**.")
            except discord.NotFound: # Message was deleted or interaction invalid
                pass
            except Exception as e:
                logger.error(f"Error trying to edit processing_message after failed power action: {e}")


async def _handle_user_server_action_selection(interaction: discord.Interaction, action_name: str, action_callback: callable):
    await interaction.response.defer(ephemeral=True)
    convoy_user_id = await get_linked_convoy_id(interaction.user.id)
    if not convoy_user_id:
        await send_error_embed(interaction, "Link Error", "Your Discord account isn't linked! Use `/link` first.")
        return

    # Get servers via Application API to get Convoy UUIDs and other rich details.
    # Client API often provides less info or uses different identifiers.
    user_servers_data = await make_api_request(
        'GET', '/servers', 'application', interaction, 
        params={'filter[user_id]': convoy_user_id, 'per_page': 100, 'include': 'node,location'} # Include for more info
    )
    if not user_servers_data or not isinstance(user_servers_data.get('data'), list):
        # make_api_request would have sent an error embed if interaction was not done.
        # If it was (e.g. by defer), and still no data, we might send a followup if possible.
        if interaction.response.is_done(): # Check if we need to send a new followup
             if not await interaction.original_response(): # Check if an error was already sent (hard to be certain)
                await send_error_embed(interaction, "API Error", "Could not retrieve your server list from the panel.")
        return
    
    servers = user_servers_data['data']
    if not servers:
        await send_info_embed(interaction, f"{action_name.capitalize()} VPS", "🤔 You don't seem to have any servers currently.", ephemeral=True)
        return

    selected_server_uuid = None
    selected_server_app_id = None # Application API ID
    selected_server_name = "Unknown Server"

    if len(servers) == 1:
        server = servers[0]
        selected_server_uuid = server.get('uuid') # Convoy UUID
        selected_server_app_id = server.get('id') # Application API internal ID
        selected_server_name = server.get('name', selected_server_uuid)
        if not selected_server_uuid or not selected_server_app_id:
            await send_error_embed(interaction, "API Data Error", "Could not get necessary identifiers for your server. Please contact support.")
            return
    else:
        view = ServerActionView(interaction, servers, action_name, f"👇 Select server to {action_name}...", f"user_{action_name}_select")
        await view.start(f"You have multiple servers. Which one do you want to **{action_name}**?")
        await view.wait()
        if view.selected_server_uuid and view.selected_server_data:
            selected_server_uuid = view.selected_server_data.get('uuid') # Ensure we get UUID
            selected_server_app_id = view.selected_server_data.get('id') # And App ID
            selected_server_name = view.selected_server_data.get('name', selected_server_uuid)
            if not selected_server_uuid or not selected_server_app_id:
                await interaction.followup.send(f"{Elookup('error')} Error: Selected server data is incomplete.", ephemeral=True)
                return
        else: 
            if not view.is_finished() and view.selected_server_uuid is None: 
                 await interaction.followup.send(f"{Elookup('cancel')} Server selection for {action_name} cancelled.", ephemeral=True)
            return 

    if selected_server_uuid and selected_server_app_id:
        # Pass all necessary IDs to the callback
        await action_callback(interaction, selected_server_uuid, selected_server_app_id, selected_server_name)
    else: 
        await send_error_embed(interaction, "Selection Process Error", "No server was properly selected or server data is missing.")


# --- Template Selection for Reinstall ---
class TemplateSelectDropdown(discord.ui.Select):
    def __init__(self, templates: List[Dict]): 
        options = []
        self._templates_map = {} 
        for t_group in templates: 
            group_name = t_group.get('name', "Unknown Group")
            for template in t_group.get('templates', {}).get('data', []): # Convoy v4 structure
                uuid = template.get('uuid')
                name = template.get('name', f"Template {uuid[:8]}")
                if uuid:
                    options.append(discord.SelectOption(label=name[:100], value=uuid, description=f"Group: {group_name} | UUID: {uuid}"[:100]))
                    self._templates_map[uuid] = name
        
        super().__init__(placeholder="Select a new template to reinstall with...", options=options[:25], disabled=not options)

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer() 
        if self.view and hasattr(self.view, 'selected_template_uuid'):
            self.view.selected_template_uuid = self.values[0]
            self.view.stop()


class ReinstallView(discord.ui.View):
    def __init__(self, interaction: discord.Interaction, templates_data: List[Dict], server_app_id: str, server_name: str):
        super().__init__(timeout=180)
        self.interaction = interaction # The interaction that _initiated_ the reinstall (e.g., from /reinstall or ManageView button)
        self.server_app_id = server_app_id # Application API ID for reinstall endpoint
        self.server_name = server_name
        self.selected_template_uuid: Optional[str] = None
        self.message: Optional[discord.WebhookMessage] = None # The message showing this view

        self.template_dropdown = TemplateSelectDropdown(templates_data)
        self.add_item(self.template_dropdown)
        
        cancel_button = discord.ui.Button(label="Cancel Reinstall Process", style=discord.ButtonStyle.grey)
        cancel_button.callback = self.cancel_reinstall_callback
        self.add_item(cancel_button)

    async def cancel_reinstall_callback(self, interaction: discord.Interaction): # interaction here is from the cancel button
        # Edit the ReinstallView message
        if self.message:
            await self.message.edit(content=f"{Elookup('cancel')} Reinstallation process cancelled for server **{self.server_name}**.", view=None, embed=None)
        else: # Fallback if message somehow lost
            await interaction.response.edit_message(content=f"{Elookup('cancel')} Reinstallation process cancelled.", view=None, embed=None)
        self.stop()

    async def start_selection(self):
        embed = discord.Embed(
            title=f"{Elookup('reinstall')} Reinstall Server: {self.server_name}",
            description=f"⚠️ **Warning:** Reinstalling will wipe all data on **{self.server_name}**.\n\n"
                        "Please select the template you wish to reinstall your server with from the dropdown menu below. This action is irreversible.",
            color=discord.Color.orange()
        )
        # self.interaction is the interaction that started the reinstall process (e.g., /reinstall command)
        # Its response should have been deferred. Use followup.
        await self.interaction.followup.send(embed=embed, view=self, ephemeral=True)
        self.message = await self.interaction.original_response() # Get the message object we just sent
    
    async def on_timeout(self):
        if self.message and self.selected_template_uuid is None: # Only if no selection made
            try:
                for item in self.children: item.disabled = True
                await self.message.edit(content=f"{Elookup('loading')} Reinstall template selection timed out for **{self.server_name}**. Please start the process again if needed.", view=self, embed=None)
            except discord.HTTPException: pass


async def reinstall_server_action(interaction: discord.Interaction, server_uuid_pterodactyl: str, server_app_id: str, server_name: str):
    # server_uuid_pterodactyl is for panel link, server_app_id for API actions
    # Fetch server details using Application API ID to find node_id
    server_details_app = await make_api_request('GET', f'/servers/{server_app_id}', 'application', interaction)
    if not server_details_app or not isinstance(server_details_app.get('data'), dict):
        await interaction.followup.send(f"{Elookup('error')} Could not fetch critical details for server **{server_name}** (ID: `{server_app_id}`) to proceed with reinstall. Node information is missing.", ephemeral=True)
        return
    
    node_id = server_details_app['data'].get('node_id')
    if not node_id:
        await interaction.followup.send(f"{Elookup('error')} Could not determine the node for server **{server_name}**. Reinstall cannot proceed without node context for templates.", ephemeral=True)
        return

    # Fetch templates available on that node (Convoy v4 endpoint example)
    templates_data_resp = await make_api_request('GET', f'/nodes/{node_id}/template-groups?include=templates', 'application', interaction)
    if not templates_data_resp or not isinstance(templates_data_resp.get('data'), list) or not templates_data_resp['data']:
        await interaction.followup.send(f"{Elookup('error')} Could not fetch reinstall templates for node `{node_id}`, or no templates are available for this node. Reinstall cannot proceed.", ephemeral=True)
        return
    
    reinstall_view = ReinstallView(interaction, templates_data_resp['data'], server_app_id, server_name)
    await reinstall_view.start_selection() # This sends its own followup message
    await reinstall_view.wait() # Wait for template selection or timeout

    if reinstall_view.selected_template_uuid:
        selected_template_name = reinstall_view.template_dropdown._templates_map.get(reinstall_view.selected_template_uuid, f"Template UUID: {reinstall_view.selected_template_uuid[:8]}")
        
        # Confirm the reinstall action
        confirm_reinstall_actual_view = ConfirmView(interaction.user.id, confirm_label=f"Yes, Reinstall with {selected_template_name[:25]}...")
        
        # Edit the ReinstallView's message to become the confirmation message
        if reinstall_view.message: 
            await reinstall_view.message.edit(
                content=f"{Elookup('warning')} **FINAL CONFIRMATION REQUIRED:**\n\n"
                        f"You are about to reinstall server **{server_name}** (ID: `{server_app_id}`) with the template:\n"
                        f"**{selected_template_name}**.\n\n"
                        f"🛑 **ALL DATA ON THIS SERVER WILL BE PERMANENTLY ERASED AND CANNOT BE RECOVERED.** 🛑\n\n"
                        f"Are you absolutely sure you want to proceed?",
                view=confirm_reinstall_actual_view, embed=None # Clear previous embed
            )
            confirm_reinstall_actual_view.message = reinstall_view.message # Link ConfirmView to this message
        else: # Fallback if ReinstallView's message object was lost (should not happen)
            await interaction.followup.send(
                 f"{Elookup('warning')} **FINAL CONFIRMATION:** Reinstall **{server_name}** with **{selected_template_name}**? ALL DATA WILL BE WIPED.",
                 view=confirm_reinstall_actual_view, ephemeral=True
            )
            confirm_reinstall_actual_view.message = await interaction.original_response()

        await confirm_reinstall_actual_view.wait() # Wait for final yes/no

        if confirm_reinstall_actual_view.confirmed:
            # The confirmation message is edited by ConfirmView to "Processing..."
            # Use Application API for reinstall, with server_app_id
            payload = {"template_uuid": reinstall_view.selected_template_uuid} # For Convoy v4
            
            # Pass the interaction that initiated the reinstall for API error handling
            reinstall_api_response = await make_api_request('POST', f'/servers/{server_app_id}/reinstall', 'application', interaction, json_data=payload)
            
            # Message to edit is confirm_reinstall_actual_view.message
            target_message_for_status = confirm_reinstall_actual_view.message
            if not target_message_for_status: # Should not happen
                target_message_for_status = await interaction.original_response()


            if reinstall_api_response is not None: # API call was successful (e.g., 204 No Content)
                if target_message_for_status:
                     await target_message_for_status.edit(content=f"{Elookup('success')} Reinstallation process has been successfully initiated for server **{server_name}** using template **{selected_template_name}**. Please allow a few minutes for the process to complete.", view=None)
                else: # Fallback if message couldn't be edited
                     await interaction.followup.send(f"{Elookup('success')} Reinstall initiated for **{server_name}**.", ephemeral=True)
            # If reinstall_api_response is None, make_api_request handled sending an error to the interaction passed to it
            # (which was the original /reinstall command's interaction or manage button interaction).
            # The "Processing..." message from ConfirmView might still be visible.
            # We might want to ensure that "Processing..." message is updated if make_api_request fails.
            # This is complex because make_api_request sends to its own interaction.
            # For now, assume make_api_request's error is sufficient.
    # Cancellation or timeout of ReinstallView or ConfirmView handled by those views by editing their messages.


# --- User Commands ---
@bot.tree.command(name="start", description=f"{Elookup('power_on')} Starts your VPS.")
async def start_vps(interaction: discord.Interaction):
    await _handle_user_server_action_selection(interaction, "start", 
        lambda inter, uuid, app_id, name: _execute_server_power_action(inter, uuid, name, "start"))

@bot.tree.command(name="stop", description=f"{Elookup('power_off')} Stops your VPS.")
async def stop_vps(interaction: discord.Interaction):
    await _handle_user_server_action_selection(interaction, "stop",
        lambda inter, uuid, app_id, name: _execute_server_power_action(inter, uuid, name, "stop"))

@bot.tree.command(name="restart", description=f"{Elookup('power_restart')} Restarts your VPS.")
async def restart_vps(interaction: discord.Interaction):
    await _handle_user_server_action_selection(interaction, "restart",
        lambda inter, uuid, app_id, name: _execute_server_power_action(inter, uuid, name, "restart"))

@bot.tree.command(name="kill", description=f"{Elookup('power_kill')} Forcefully stops (kills) your VPS process.")
async def kill_vps(interaction: discord.Interaction):
    await _handle_user_server_action_selection(interaction, "kill",
        lambda inter, uuid, app_id, name: _execute_server_power_action(inter, uuid, name, "kill"))

@bot.tree.command(name="reinstall", description=f"{Elookup('reinstall')} Reinstalls your VPS with a chosen template (ALL DATA WIPED).")
async def reinstall_vps(interaction: discord.Interaction):
    # action_callback expects (interaction, server_uuid_pterodactyl, server_app_id, server_name)
    await _handle_user_server_action_selection(interaction, "reinstall", reinstall_server_action)


@bot.tree.command(name="delete", description=f"{Elookup('delete')} Deletes your VPS from the panel (Requires Confirmation).")
async def delete_vps(interaction: discord.Interaction):
    # action_callback for _handle_user_server_action_selection needs:
    # (interaction_from_selection_view, pterodactyl_uuid, application_api_id, server_name)
    async def delete_action_callback(selection_interaction: discord.Interaction, server_uuid_pterodactyl: str, server_app_id: str, server_name: str):
        confirm_view = ConfirmView(selection_interaction.user.id, confirm_label="Yes, PERMANENTLY Delete")
        
        # Send a new confirmation message using the followup from the selection interaction
        # This 'selection_interaction' is the original /delete command's interaction
        await selection_interaction.followup.send(
            f"{Elookup('warning')} **DANGER ZONE: Confirm Server Deletion**\n\n"
            f"You are about to permanently delete server **{server_name}** (ID: `{server_app_id}`, UUID: `{server_uuid_pterodactyl[:8]}`).\n\n"
            f"🛑 **THIS ACTION IS IRREVERSIBLE. ALL SERVER DATA, BACKUPS, AND CONFIGURATIONS WILL BE LOST FOREVER.** 🛑\n\n"
            f"Are you absolutely certain you wish to proceed with deleting this server?",
            view=confirm_view, ephemeral=True
        )
        confirm_view.message = await selection_interaction.original_response() # Store this new confirmation message
        await confirm_view.wait()

        if confirm_view.confirmed:
            # ConfirmView will edit its message to "Processing..."
            # Use Application API for delete, using server_app_id (internal ID)
            # Pass the selection_interaction for API error handling
            response_data = await make_api_request('DELETE', f'/servers/{server_app_id}', 'application', selection_interaction)
            
            target_message_for_status = confirm_view.message # The message that says "Processing..."
            if not target_message_for_status: # Fallback
                target_message_for_status = await selection_interaction.original_response()

            if response_data is not None: # Deletion command accepted by API (e.g., 204 No Content)
                if target_message_for_status:
                    await target_message_for_status.edit(content=f"{Elookup('success')} Successfully initiated deletion for server **{server_name}**. It will be removed from the panel shortly.", view=None)
            # If response_data is None, make_api_request has already handled sending an error message
            # using selection_interaction, which would update/replace the "Processing..." message.
            
    await _handle_user_server_action_selection(interaction, "delete", delete_action_callback)


# --- /servers (formerly list_my_servers) ---
@bot.tree.command(name="servers", description=f"{Elookup('server','📄')} Lists all servers linked to your panel account.")
async def list_my_servers(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=False) # Non-ephemeral for server list
    user_panel_id = await get_linked_convoy_id(interaction.user.id)
    if not user_panel_id:
        await send_error_embed(interaction, "Account Not Linked", "Your Discord account is not linked to a panel account. Please use `/link` first.", ephemeral=True)
        return
    
    params = {'page': 1, 'per_page': 5, 'filter[user_id]': user_panel_id, 'include': 'node,location'}
    response_data = await make_api_request('GET', '/servers', 'application', interaction, params=params)
   
    if response_data and isinstance(response_data.get('data'), list):
        if not response_data['data'] and response_data.get('meta', {}).get('pagination', {}).get('total', 0) == 0:
            await send_info_embed(interaction, "My Servers", "🍃 You currently don't have any servers linked to your account on the panel.", ephemeral=False)
            return
        # Pass 'application' as api_type, as we are fetching from Application API
        view = ServerListView(interaction, response_data, items_per_page=5, title_prefix="My Servers", api_type='application')
        view.user_panel_id_filter = user_panel_id # Store filter for pagination
        await view.start() # This will send its own followup
    elif response_data is not None: # API call succeeded but data format is wrong
         if not interaction.response.is_done():
            await send_error_embed(interaction, "API Format Error", f"Received unexpected data format when fetching your servers: ```{str(response_data)[:1000]}```", ephemeral=True)
    # If response_data is None, make_api_request already sent an error embed if interaction wasn't done


# --- /manage Command ---
class ManageServerView(discord.ui.View):
    def __init__(self, interaction: discord.Interaction, server_uuid_pterodactyl: str, server_app_id: str, server_name: str):
        super().__init__(timeout=600) # 10 min timeout
        self.interaction_context = interaction # Original /manage command interaction
        self.server_uuid_pterodactyl = server_uuid_pterodactyl # Convoy UUID for Client API & panel links
        self.server_app_id = server_app_id # Application API ID for app-specific actions like reinstall/delete
        self.server_name = server_name
        self.message: Optional[discord.WebhookMessage] = None # The message displaying this view

        self._add_buttons()

    def _add_buttons(self):
        self.add_item(discord.ui.Button(label="Start", emoji=Elookup("power_on"), style=discord.ButtonStyle.green, custom_id="manage_start", row=0))
        self.add_item(discord.ui.Button(label="Stop", emoji=Elookup("power_off"), style=discord.ButtonStyle.red, custom_id="manage_stop", row=0))
        self.add_item(discord.ui.Button(label="Restart", emoji=Elookup("power_restart"), style=discord.ButtonStyle.blurple, custom_id="manage_restart", row=0))
        self.add_item(discord.ui.Button(label="Kill", emoji=Elookup("power_kill"), style=discord.ButtonStyle.secondary, custom_id="manage_kill", row=0))
        
        self.add_item(discord.ui.Button(label="Reinstall", emoji=Elookup("reinstall"), style=discord.ButtonStyle.grey, custom_id="manage_reinstall", row=1))
        self.add_item(discord.ui.Button(label="Delete", emoji=Elookup("delete"), style=discord.ButtonStyle.danger, custom_id="manage_delete", row=1))
        self.add_item(discord.ui.Button(label="Refresh Stats", emoji="🔄", style=discord.ButtonStyle.blurple, custom_id="manage_refresh", row=1))
        
        panel_link_button = discord.ui.Button(label="Open in Panel", emoji="🔗", style=discord.ButtonStyle.link, 
                                             url=f"{PANEL_BASE_URL}/server/{self.server_uuid_pterodactyl}", row=1)
        self.add_item(panel_link_button)


        for item in self.children:
            if isinstance(item, discord.ui.Button) and item.style != discord.ButtonStyle.link: # Don't assign callback to link buttons
                item.callback = self.button_callback

    async def _fetch_and_create_embed(self, interaction_for_api: discord.Interaction) -> discord.Embed:
        # Get static details from Application API (like limits, node) using server_app_id
        app_server_details_resp = await make_api_request('GET', f'/servers/{self.server_app_id}?include=node,location', 'application', interaction_for_api)

        # Get live stats from Client API (Convoy standard) using server_uuid_pterodactyl
        client_server_usage_resp = await make_api_request('GET', f'/servers/{self.server_uuid_pterodactyl}/resources', 'client', interaction_for_api)

        embed = discord.Embed(title=f"{Elookup('manage')} Manage Server: {self.server_name}", color=discord.Color.dark_theme(), timestamp=datetime.now(timezone.utc))
        embed.add_field(name="🏷️ Server Name", value=f"`{self.server_name}`", inline=True)
        embed.add_field(name="🆔 App ID", value=f"`{self.server_app_id}`", inline=True)
        embed.add_field(name="🔑 Convoy UUID", value=f"`{self.server_uuid_pterodactyl}`", inline=True)

        node_name_display = "N/A"
        location_display = "N/A"
        server_max_cpu_from_app = "N/A" # Convoy: CPU Limit %
        max_mem_mb = "N/A"
        max_disk_mb = "N/A"
        status_from_app = 'Unknown' # Status from Application API (can be 'installing', 'suspended')

        if app_server_details_resp and isinstance(app_server_details_resp.get('data'), dict):
            app_data = app_server_details_resp['data']
            limits_data = app_data.get('limits', {})
            server_max_cpu_from_app = limits_data.get('cpu', 'N/A') # Convoy: CPU % (e.g., 100 for 1 core)
            max_mem_mb = limits_data.get('memory', 'N/A') 
            max_disk_mb = limits_data.get('disk', 'N/A') 

            status_value_app = app_data.get('status') # e.g. null (active), 'installing', 'suspended'
            if status_value_app: status_from_app = str(status_value_app).capitalize()
            elif app_data.get('suspended'): status_from_app = 'Suspended' # Another way to check suspension
            else: status_from_app = 'Active' # Default if no specific status and not suspended

            node_obj = app_data.get('node', app_data.get('relationships',{}).get('node',{}).get('attributes'))
            if isinstance(node_obj, dict): node_name_display = node_obj.get('name', str(app_data.get('node_id')))
            else: node_name_display = str(app_data.get('node_id', "N/A"))
            
            loc_obj = app_data.get('location', app_data.get('relationships',{}).get('location',{}).get('attributes'))
            if isinstance(loc_obj, dict): location_display = loc_obj.get('short', loc_obj.get('long', "N/A"))
            else: location_display = "N/A"

        embed.add_field(name=f"{Elookup('cpu')} CPU Limit", value=f"`{server_max_cpu_from_app}%`", inline=True)
        embed.add_field(name=f"{Elookup('ram')} Max RAM", value=f"`{max_mem_mb} MB`", inline=True)
        embed.add_field(name=f"{Elookup('disk')} Max Disk", value=f"`{max_disk_mb} MB`", inline=True)
        embed.add_field(name="📍 Location", value=f"`{location_display}`", inline=True)
        embed.add_field(name="📦 Node", value=f"`{node_name_display}`", inline=True)
        
        # --- Live Stats from Client API ---
        current_state_client = "N/A" # Convoy: 'running', 'offline', 'starting', 'stopping'
        cpu_usage_str, ram_usage_str, disk_usage_str = "N/A", "N/A", "N/A"
        status_emoji = Elookup("unknown_status", "❓")

        if client_server_usage_resp and isinstance(client_server_usage_resp.get('attributes'), dict):
            usage_attrs = client_server_usage_resp['attributes']
            current_state_client = usage_attrs.get('current_state', 'N/A').capitalize()

            if current_state_client == 'Running': status_emoji = Elookup("online","🟢")
            elif current_state_client == 'Offline': status_emoji = Elookup("offline","🔴")
            elif current_state_client == 'Starting': status_emoji = Elookup("starting","🟡")
            elif current_state_client == 'Stopping': status_emoji = Elookup("stopping","🟠")
            
            # If app API says suspended, override client API status display
            if status_from_app == 'Suspended':
                current_state_client = 'Suspended'
                status_emoji = Elookup("offline", "🔴") # Or a specific suspend emoji

            resources = usage_attrs.get('resources', {})
            cpu_abs_perc = resources.get('cpu_absolute') # Absolute CPU % used across all cores allocated
            mem_bytes = resources.get('memory_bytes', 0)
            disk_bytes = resources.get('disk_bytes', 0)

            if cpu_abs_perc is not None: cpu_usage_str = f"{cpu_abs_perc:.1f}%"
            ram_usage_str = f"{format_size(mem_bytes)}"
            if isinstance(max_mem_mb, (int, float)) and max_mem_mb > 0: ram_usage_str += f" / {max_mem_mb} MB"
            disk_usage_str = f"{format_size(disk_bytes)}"
            if isinstance(max_disk_mb, (int, float)) and max_disk_mb > 0: disk_usage_str += f" / {max_disk_mb} MB"
        else: # Fallback if client API fails, use app API status
            current_state_client = status_from_app # Use status from app API
            if status_from_app == 'Active': status_emoji = Elookup("unknown_status", "❓") # Active but can't get live state
            elif status_from_app == 'Suspended': status_emoji = Elookup("offline", "🔴")
            elif status_from_app == 'Installing': status_emoji = Elookup("starting", "🛠️")


        embed.add_field(name="📊 Live Status", value=f"{status_emoji} **{current_state_client}**", inline=True)
        embed.add_field(name=f"{Elookup('cpu')} CPU Usage", value=f"`{cpu_usage_str}`", inline=True)
        embed.add_field(name=f"{Elookup('ram')} RAM Usage", value=f"`{ram_usage_str}`", inline=True)
        embed.add_field(name=f"{Elookup('disk')} Disk Usage", value=f"`{disk_usage_str}`", inline=True)

        embed.set_footer(text=f"Stats Refreshed: {discord.utils.format_dt(embed.timestamp, 'T')}")
        return embed

    async def start_management(self):
        loading_embed = discord.Embed(title=f"{Elookup('loading')} Loading server management panel for {self.server_name}...", color=discord.Color.light_grey())
        
        # interaction_context is the /manage command interaction, its response is deferred.
        # We send the loading message as a followup.
        await self.interaction_context.followup.send(embed=loading_embed, ephemeral=True)
        try:
            self.message = await self.interaction_context.original_response() # Get the WebhookMessage we just sent
        except discord.NotFound:
            logger.error("Failed to get original_response for ManageServerView message after followup.")
            await self.interaction_context.followup.send(f"{Elookup('error')} Critical error: Failed to initialize the management session interface.", ephemeral=True)
            return
        except Exception as e:
            logger.error(f"Unexpected error getting original_response for ManageServerView: {e}")
            await self.interaction_context.followup.send(f"{Elookup('error')} An unexpected error occurred initializing the management interface.", ephemeral=True)
            return


        if not self.message:
            logger.error("ManageServerView.message is None after attempting to send initial loading message.")
            return # Cannot proceed without a message to update

        try:
            # Fetch initial data using the interaction_context of the /manage command
            final_embed = await self._fetch_and_create_embed(self.interaction_context)
            await self.message.edit(embed=final_embed, view=self)
        except discord.HTTPException as e:
            logger.error(f"Failed to edit manage message with initial full embed: {e}")
            try: # Try to update the loading message with an error
                await self.message.edit(embed=discord.Embed(title=f"{Elookup('error')} Error Loading Details", description="Could not fully load server details. Some information may be missing. Try refreshing.", color=discord.Color.red()), view=self)
            except: pass # Final attempt
        except Exception as e:
            logger.error(f"Unexpected error during initial data fetch for ManageServerView: {e}", exc_info=True)


    async def button_callback(self, interaction: discord.Interaction): # This interaction is from a button press
        custom_id = interaction.data['custom_id']
        action = custom_id.split('_')[1] # e.g., "start" from "manage_start"
        
        # Defer the button interaction (ephemeral if a message will be sent, otherwise not needed if just editing main view)
        # For power actions, we send a new ephemeral followup, so defer this button interaction.
        # For refresh, we just edit the main view, so defer is good.
        # For reinstall/delete, they have their own multi-step interactions.
        if action not in ["reinstall", "delete"]: # Reinstall/delete handle their own deferrals
            await interaction.response.defer(ephemeral=True) # Keep it true to allow followup messages for actions

        if action in ["start", "stop", "restart", "kill"]:
            # Pass the button's interaction for _execute_server_power_action to send its ephemeral followup
            await _execute_server_power_action(interaction, self.server_uuid_pterodactyl, self.server_name, action)
            # After action, refresh the main manage view embed
            if self.message:
                try:
                    # Use interaction_context (original /manage command's) for fetching embed data
                    # but the button's interaction (interaction) has been used for the power action's ephemeral message
                    new_embed = await self._fetch_and_create_embed(self.interaction_context) 
                    await self.message.edit(embed=new_embed, view=self)
                except discord.HTTPException as e:
                    logger.error(f"Failed to refresh ManageServerView after power action ({action}): {e}")
                    # Button interaction's ephemeral message already sent by _execute_server_power_action

        elif action == "reinstall":
            # Reinstall needs server_app_id for the API call
            # Pass the button's interaction (interaction) to reinstall_server_action
            # It will handle its own deferrals and followups.
            await reinstall_server_action(interaction, self.server_uuid_pterodactyl, self.server_app_id, self.server_name)
            if self.message: # Attempt to refresh main manage view after reinstall modal flow finishes
                try:
                    new_embed = await self._fetch_and_create_embed(self.interaction_context)
                    await self.message.edit(embed=new_embed, view=self)
                except discord.HTTPException as e:
                    logger.error(f"Failed to refresh ManageServerView after reinstall action attempt: {e}")

        elif action == "delete":
            # Delete needs server_app_id for the API call
            # Pass the button's interaction (interaction) to the delete process
            confirm_view = ConfirmView(interaction.user.id, confirm_label="Yes, PERMANENTLY Delete")
            delete_confirm_msg = await interaction.followup.send( # Use button interaction's followup
                f"{Elookup('warning')} **DANGER: Confirm Deletion**\nDelete server **{self.server_name}** (App ID: `{self.server_app_id}`)?\n**THIS IS IRREVERSIBLE AND ALL DATA WILL BE LOST.**",
                view=confirm_view, ephemeral=True
            )
            confirm_view.message = delete_confirm_msg
            await confirm_view.wait()

            if confirm_view.confirmed: # User confirmed deletion
                # ConfirmView edits its own message to "Processing..."
                # Use button interaction for the delete API call, with server_app_id
                response_data = await make_api_request('DELETE', f'/servers/{self.server_app_id}', 'application', interaction) 
                
                target_msg_for_status = confirm_view.message # Message that says "Processing..."
                if not target_msg_for_status: target_msg_for_status = await interaction.original_response()


                if response_data is not None: # Deletion accepted by API
                    if target_msg_for_status:
                        await target_msg_for_status.edit(content=f"{Elookup('success')} Deletion initiated for **{self.server_name}**. This management panel will no longer function for this server.", view=None)
                    
                    # Disable all buttons on the main manage view and indicate server is gone
                    for item_child in self.children: item_child.disabled = True
                    if self.message: # The main manage view message
                        try:
                             closed_embed = discord.Embed(title=f"{Elookup('delete')} Server Deleted: {self.server_name}", description="This server has been scheduled for deletion and this management panel is now closed.", color=discord.Color.dark_grey())
                             await self.message.edit(embed=closed_embed, view=self) # Update main view with disabled buttons
                        except discord.HTTPException: pass
                    self.stop() # Stop the ManageServerView
            # If API error, make_api_request handles it for 'interaction' (button interaction)
            # ConfirmView's "Processing..." message might remain.

        elif action == "refresh":
            if self.message:
                refresh_indicator_msg = await interaction.followup.send(f"{Elookup('loading')} Refreshing server statistics for **{self.server_name}**...",ephemeral=True)
                try:
                    # Use button's interaction for API calls during refresh, as it's the most current context
                    new_embed = await self._fetch_and_create_embed(interaction) 
                    await self.message.edit(embed=new_embed, view=self) # Update main view
                except discord.HTTPException as e:
                     logger.error(f"Failed to refresh ManageServerView: {e}")
                     await interaction.followup.send(f"{Elookup('error')} Failed to refresh server statistics. Please try again in a moment.", ephemeral=True) # Send another followup
                finally: # Clean up the "Refreshing..." indicator message
                    try: await refresh_indicator_msg.delete()
                    except discord.HTTPException: pass


    async def on_timeout(self):
        if self.message:
            try:
                timeout_embed = discord.Embed(
                    title=f"{Elookup('manage')} Manage Server: {self.server_name}", 
                    description=f"{Elookup('loading')} This interactive management session has timed out. Use `/manage` again to start a new session.", 
                    color=discord.Color.dark_grey()
                )
                for item in self.children: item.disabled = True
                await self.message.edit(embed=timeout_embed, view=self)
            except discord.HTTPException as e:
                logger.warning(f"HTTPException during ManageServerView on_timeout: {e}")
            except Exception as e:
                logger.error(f"Unexpected error during ManageServerView on_timeout: {e}", exc_info=True)


@bot.tree.command(name="manage", description=f"{Elookup('manage')} Interactively manage one of your VPS (stats, power, etc.).")
async def manage_vps(interaction: discord.Interaction):
    # Callback for _handle_user_server_action_selection:
    # Needs (interaction_from_selection_view, pterodactyl_uuid, application_api_id, server_name)
    async def manage_action_callback(selection_interaction: discord.Interaction, server_uuid_pterodactyl: str, server_app_id: str, server_name: str):
        # selection_interaction is the original /manage command's interaction.
        # server_uuid_pterodactyl is the Convoy UUID from Application API list.
        # server_app_id is the Application API internal ID.
        
        manage_view_instance = ManageServerView(selection_interaction, server_uuid_pterodactyl, server_app_id, server_name)
        await manage_view_instance.start_management() # This sends its own followup via selection_interaction

    # Pass the original /manage command's interaction to _handle_user_server_action_selection
    # It will defer it and use its followup for the server selection view.
    await _handle_user_server_action_selection(interaction, "manage", manage_action_callback)


# --- Account Linking Commands ---
class LinkAccountModal(ui.Modal, title='🔗 Link Panel Account'):
    panel_email = ui.TextInput(label='Panel Account Email Address', placeholder='Enter the email address used for your panel account', required=True, style=discord.TextStyle.short)
    
    async def on_submit(self, interaction: discord.Interaction): # interaction is from the modal submission
        await interaction.response.defer(ephemeral=True, thinking=True) # Defer and show thinking
        email_to_check = self.panel_email.value.strip()
        
        # Use Application API to find user by email
        params = {'filter[email]': email_to_check}
        user_data_response = await make_api_request('GET', '/users', 'application', interaction, params=params)

        if not user_data_response or not isinstance(user_data_response.get('data'), list):
            # make_api_request would have sent an error if interaction wasn't done.
            # If it was (due to defer), we might need a followup.
            # Let's assume make_api_request handles it or sends a generic error.
            # If it did send an error, this modal will just close.
            # If it didn't, we ensure one is sent here if needed.
            if interaction.response.is_done(): # If make_api_request errored and interaction was already responded
                 if not await interaction.original_response(): # Check if an error was already sent by make_api_request
                    await send_error_embed(interaction, "API Error", "Could not fetch user data from the panel or the response was in an unexpected format.")
            self.stop()
            return
            
        user_list_from_api = user_data_response['data']
        if not user_list_from_api:
            await send_error_embed(interaction, "User Not Found", f"No panel user account was found with the email address `{email_to_check}`. Please ensure the email is correct. If you don't have an account, an admin may need to create one for you using `/admin createuser`.")
            self.stop()
            return
        
        if len(user_list_from_api) > 1:
            await send_error_embed(interaction, "Multiple Users Found", f"Multiple panel user accounts were found with the email `{email_to_check}`. This is an unusual situation. Please contact an administrator for assistance.")
            self.stop()
            return
            
        convoy_panel_user = user_list_from_api[0]
        convoy_panel_user_id = convoy_panel_user.get('id')
        convoy_panel_username = convoy_panel_user.get('username', convoy_panel_user.get('name', 'N/A')) # Prefer username
        
        if not convoy_panel_user_id:
            await send_error_embed(interaction, "API Data Error", "Found the user on the panel, but their User ID is missing in the API response. Please contact an administrator.")
            self.stop()
            return
            
        await link_user_account(interaction.user.id, str(convoy_panel_user_id))
        await send_success_embed(interaction, "Account Successfully Linked!", f"{Elookup('success')} Your Discord account is now linked to the panel user **{convoy_panel_username}** (Panel User ID: `{convoy_panel_user_id}`). You can now use user-specific VPS commands.", ephemeral=True)
        self.stop()

    async def on_error(self, interaction: discord.Interaction, error: Exception):
        logger.error(f"Error occurred in LinkAccountModal: {error}", exc_info=True)
        try:
            # The interaction might already be responded to by on_submit's defer.
            # Try a followup if response is done.
            msg_content = f"{Elookup('error')} An unexpected error occurred while processing the account linking form. Please try again."
            if interaction.response.is_done():
                await interaction.followup.send(msg_content, ephemeral=True)
            else: # Should not happen if on_submit defers.
                await interaction.response.send_message(msg_content, ephemeral=True)
        except discord.HTTPException: 
            pass # Best effort to inform user
        self.stop()


@bot.tree.command(name="link", description=f"{Elookup('link')} Links your Discord account to your Panel user account.")
async def link_account_cmd(interaction: discord.Interaction): 
    existing_link_id = await get_linked_convoy_id(interaction.user.id)
    if existing_link_id:
        # Fetch panel user details to show name
        panel_user_details = await make_api_request('GET', f'/users/{existing_link_id}', 'application', interaction=None) # No interaction for this simple check
        panel_username_display = f"Panel User ID: {existing_link_id}" # Default
        if panel_user_details and isinstance(panel_user_details.get('data'), dict):
            panel_username_display = panel_user_details['data'].get('username', panel_user_details['data'].get('name', f'ID: {existing_link_id}'))
        
        await send_info_embed(interaction, "Account Already Linked", f"Your Discord account is already linked to the panel account: **{panel_username_display}**. If this is incorrect or you wish to change it, please use the `/unlink` command first.", ephemeral=True)
        return
        
    modal = LinkAccountModal()
    await interaction.response.send_modal(modal) # This handles the initial response

@bot.tree.command(name="unlink", description=f"{Elookup('unlink')} Unlinks your Discord account from its associated Panel account.")
async def unlink_account_cmd(interaction: discord.Interaction): 
    await interaction.response.defer(ephemeral=True)
    if await unlink_user_account(interaction.user.id):
        await send_success_embed(interaction, "Account Successfully Unlinked", f"{Elookup('success')} Your Discord account has been unlinked from the panel. You will need to use `/link` again to re-associate it.", ephemeral=True)
    else:
        await send_info_embed(interaction, "Account Not Linked", "Your Discord account is not currently linked to any panel user account. There's nothing to unlink.", ephemeral=True)


# --- Utility Commands ---
@bot.tree.command(name="credits", description=f"{Elookup('credits')} Shows bot creator and project credits.")
async def credits_cmd(interaction: discord.Interaction): 
    embed = discord.Embed(title=f"{Elookup('credits')} Rn Nodes Bot - Credits", description="This bot was lovingly crafted for the Rn Nodes community by:", color=discord.Color.gold())
    embed.add_field(name="👑 nahmo", value="Discord: `naj.hq`", inline=False)
    embed.add_field(name="👑 identitytheft", value="Discord: `identitytheft.io`", inline=False)
    embed.add_field(name="✨ RN Nodes Project", value="[Visit our Website](https://vm.rnnodes.qzz.io)", inline=False)
    if bot.user and bot.user.avatar:
        embed.set_thumbnail(url=bot.user.avatar.url)
    embed.set_footer(text="Thank you for using the Rn Nodes Bot! Your Vision, Virtually Realized. ❤️")
    await interaction.response.send_message(embed=embed, ephemeral=False)

usage_counter = {} 
@bot.tree.command(name="getfreevps", description=f"{Elookup('boost_plan','🎁')} [EXCLUSIVE OFFER!] Click here to claim your FREE 128GB RAM VPS!") 
async def get_fake_vps(interaction: discord.Interaction):
    user_id = interaction.user.id
    usage_counter[user_id] = usage_counter.get(user_id, 0) + 1
    
    # First time, send an ephemeral "processing" message
    if usage_counter[user_id] == 1:
        await interaction.response.send_message(f"{Elookup('loading','⏳')} Verifying your eligibility for the 128GB RAM Ultra-VPS... this may take a moment!", ephemeral=True)
        # Optional: await asyncio.sleep(2) # Short delay
        # Then edit to the troll or let the second interaction trigger DM
        try: # Attempt to edit the ephemeral message after a short delay
            await asyncio.sleep(random.uniform(1.5, 3.0))
            await interaction.edit_original_response(content="Hmm, it seems there was a slight issue with the quantum entanglement of your request. Try invoking the command one more time to re-align the server-reality matrix!")
        except discord.HTTPException: # Original ephemeral message might have disappeared if user dismissed it
            pass 
        return

    # Second (or more) time, send the DM and a different ephemeral message
    elif usage_counter[user_id] >= 2:
        try:
            dm_embed = discord.Embed(
                title="⚠️ Oops, You've Been Trolled! 🤣",
                description="Did you *really* think we were giving away 128GB RAM VPS like candy? Gotcha! 😉\n\n"
                            "While that specific offer was just a bit of fun, we **do** have amazing actual plans! Check them out with `/plans`.",
                color=discord.Color.orange()
            )
            dm_embed.set_image(url="https://media.tenor.com/aSkdq3IU0g0AAAAM/laughing-cat.gif")
            dm_embed.set_footer(text="Thanks for being a good sport!")
            await interaction.user.send(embed=dm_embed)
            
            await interaction.response.send_message(f"{Elookup('success','✅')} Your 'Ultra-VPS' activation signal has been... *intercepted* by intergalactic space hamsters! Check your DMs for a... 'status update'. 😉", ephemeral=True)
        except discord.Forbidden: # Cannot DM user
            await interaction.response.send_message(f"{Elookup('error','❌')} Error: Could not transmit the 'Ultra-VPS' activation to your private comms channel (DMs disabled?). But let's be real, did you fall for that? 😂 Use `/plans` for real deals!", ephemeral=True)
        except discord.HTTPException: # Other HTTP error
            await interaction.response.send_message(f"{Elookup('error','❌')} A cosmic ray seems to have interfered with the 'Ultra-VPS' deployment. Try `/plans` for actual VPS info!", ephemeral=True)
        
        # Reset counter after the troll to allow it again later if desired, or keep it high
        if usage_counter[user_id] > 5: # ARn spamming DMs if user keeps trying
            usage_counter[user_id] = 1 # Reset after a few tries

# --- Help Command with Dropdown ---
class HelpCategorySelect(discord.ui.Select):
    def __init__(self, parent_view: 'HelpView'):
        self.parent_view = parent_view
        options = [
            discord.SelectOption(label=f"{Elookup('admin','👑')} Admin Commands", value="admin", description="Commands for users with the VPS Creator role."),
            discord.SelectOption(label=f"{Elookup('user','👤')} User VPS Commands", value="user", description="Commands for managing your own VPS instances."),
            discord.SelectOption(label=f"{Elookup('utility','🛠️')} Utility Commands", value="utility", description="General helpful and informational commands."),
            discord.SelectOption(label=f"{Elookup('owner','🤖')} Bot Owner Commands", value="owner", description="Special commands reserved for the Bot Owner."),
        ]
        super().__init__(placeholder="Select a command category to learn more...", options=options, min_values=1, max_values=1)

    async def callback(self, interaction: discord.Interaction): # interaction is from select
        # Defer the select interaction as we are editing the original message
        await interaction.response.defer() 
        await self.parent_view.update_help_embed(interaction, self.values[0])

class HelpView(discord.ui.View):
    def __init__(self, interaction_user_id: int):
        super().__init__(timeout=300) # 5 minutes
        self.interaction_user_id = interaction_user_id
        self.message: Optional[discord.Message] = None # Will store the original help message
        self.add_item(HelpCategorySelect(self))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.interaction_user_id:
            await interaction.response.send_message(f"{Elookup('error')} This help menu is not for you! Please use `/help` yourself.", ephemeral=True)
            return False
        return True
    
    def get_commands_for_category(self, category: str) -> List[Dict[str, str]]:
        commands_map = {
            "admin": [
                {"name": "/admin serverlist", "desc": "Lists all servers on the panel with pagination."},
                {"name": "/admin assign `server_identifier` `user`", "desc": "Assigns an existing panel server to a different Discord user (must be linked)."},
                {"name": "/admin create `[assign_to]` `[node_id]` `[template_uuid]`", "desc": "Guides through admin VPS creation, optionally assigning to a user."},
                {"name": "/admin createuser `discord_user` `email` `[is_admin]`", "desc": "Creates a new user account on the panel and links it to the Discord user."},
                {"name": "/admin suspend", "desc": "Interactively select and suspend a server on the panel."},
                {"name": "/admin unsuspend", "desc": "Interactively select and unsuspend a server on the panel."},
                {"name": "/admin deleteserver", "desc": "Interactively select and delete a server from the panel (irreversible)."},
            ],
            "user": [
                {"name": "/create", "desc": "Initiates VPS creation based on available reward or paid plans (requires admin approval for reward plans)."},
                {"name": "/link", "desc": "Links your Discord account to your panel user account via email."},
                {"name": "/unlink", "desc": "Unlinks your Discord account from the panel user account."},
                {"name": "/servers", "desc": "Lists all servers currently linked to your panel account."},
                {"name": "/manage", "desc": "Opens an interactive panel to manage one of your VPS (stats, power, reinstall, delete)."},
                {"name": "/start", "desc": "Starts one of your VPS (interactive selection if multiple)."},
                {"name": "/stop", "desc": "Stops one of your VPS (interactive selection if multiple)."},
                {"name": "/restart", "desc": "Restarts one of your VPS (interactive selection if multiple)."},
                {"name": "/kill", "desc": "Forcefully stops (kills) one of your VPS (interactive selection if multiple)."},
                {"name": "/reinstall", "desc": "Reinstalls one of your VPS with a chosen template (ALL DATA WIPED, requires confirmation)."},
                {"name": "/delete", "desc": "Deletes one of your VPS from the panel (ALL DATA WIPED, requires confirmation)."},
            ],
            "utility": [
                {"name": "/plans", "desc": "Displays available VPS plans (Paid, Boost Rewards, Invite Rewards) with their specifications."},
                {"name": "/credits", "desc": "Shows credits for the bot creators and the Rn Nodes project."},
                {"name": "/help", "desc": "Shows this interactive help message with command categories."},
                {"name": "/generatepassword `[length]`", "desc": "Generates a strong, compliant password (default 16 chars, 8-50). Ephemeral."},
                {"name": "/nodes", "desc": "Shows the current status and resource usage of panel compute nodes."},
                {"name": "/convert `value` `from_unit` `to_unit`", "desc": "Converts digital storage units (e.g., GB to MB, TB to GiB)."},
                {"name": "/links", "desc": "Shows a list of useful links related to Rn Nodes (Panel, Website, Discord)."},
                {"name": "/getfreevps", "desc": "[TOP SECRET] An amazing offer you can't refuse... or can you?"},
            ],
            "owner": [
                {"name": "/toggleboostrewards `enable`", "desc": "Globally enables or disables server boost reward plans."},
                {"name": "/toggleinviterewards `enable`", "desc": "Globally enables or disables invite-based reward plans."},
                {"name": "/sync `scope`", "desc": "Synchronizes application (slash) commands with Discord (guild or global)."},
            ]
        }
        # Filter commands based on actual toggles for reward plans
        if category == "user":
            user_cmds = commands_map["user"]
            if not (BOOST_REWARDS_ENABLED or INVITE_REWARDS_ENABLED_GLOBAL or PAID_PLANS_DATA): # If no way to get a plan
                user_cmds = [cmd for cmd in user_cmds if cmd['name'] != '/create']
            commands_map["user"] = user_cmds
        
        return commands_map.get(category, [])

    async def update_help_embed(self, interaction: discord.Interaction, category: str): # interaction is from select
        embed = discord.Embed(title=f"{Elookup('help', '🤖')} Rn Nodes Bot Help - {category.title()} Commands",
                              description=f"Here are the commands available in the **{category.title()}** category.\nArguments in `<angle brackets>` are required, `[square brackets]` are optional.",
                              color=discord.Color.purple())
        if bot.user and bot.user.avatar: embed.set_thumbnail(url=bot.user.avatar.url)
        
        cmds_in_category = self.get_commands_for_category(category)
        if not cmds_in_category:
            embed.description = "No commands found in this category, or the category is invalid."
        else:
            for cmd_info in cmds_in_category:
                embed.add_field(name=cmd_info['name'], value=f"`└─` {cmd_info['desc']}", inline=False)
        
        embed.set_footer(text="Use the dropdown menu above to explore other command categories. This help message is interactive.")
        if self.message: # Edit the persistent message
            try:
                await self.message.edit(embed=embed, view=self) 
            except discord.HTTPException as e:
                logger.error(f"Failed to edit help message: {e}")
                # Fallback: send new if edit fails (though interaction is from select, so followup is tricky)
                # For simplicity, we'll assume edit works or user retries /help
        else: # Should not happen if start() was called correctly
            logger.warning("HelpView.message was None during update_help_embed.")
            # This case is problematic as 'interaction' is from the select, not the original command.
            # A robust solution would store original command interaction or pass it.


    async def start(self, interaction: discord.Interaction): # interaction is the original /help command
        # Initial embed before category selection
        embed = discord.Embed(title=f"{Elookup('help', '🤖')} Rn Nodes Bot - Command Help",
                              description="Welcome to the Rn Nodes Bot! I'm here to help you manage your VPS and interact with the panel.\n\n"
                                          "Select a command category from the dropdown menu below to see detailed information about available commands.",
                              color=discord.Color.purple())
        if bot.user and bot.user.avatar: embed.set_thumbnail(url=bot.user.avatar.url)
        embed.set_footer(text="Your Vision, Virtually Realized. | This help menu is interactive.")
        
        # Send the initial message (non-ephemeral so it persists for interaction)
        await interaction.response.send_message(embed=embed, view=self, ephemeral=False)
        self.message = await interaction.original_response() # Store the message we sent

    async def on_timeout(self):
        if self.message:
            try:
                for item in self.children: # Disable all components (e.g., the select dropdown)
                    if hasattr(item, 'disabled'): item.disabled = True
                
                timeout_embed = discord.Embed(
                    title=f"{Elookup('loading')} Help Session Timed Out",
                    description="This interactive help session has timed out. Please use `/help` again if you need assistance.",
                    color=discord.Color.dark_grey()
                )
                if self.message.embeds: # Try to keep thumbnail if exists
                    timeout_embed.set_thumbnail(url=self.message.embeds[0].thumbnail.url if self.message.embeds[0].thumbnail else None)

                await self.message.edit(embed=timeout_embed, view=self) # Update message with timeout info and disabled view
            except discord.HTTPException as e:
                logger.warning(f"HTTPException during HelpView on_timeout: {e}")
            except Exception as e:
                logger.error(f"Unexpected error during HelpView on_timeout: {e}", exc_info=True)


@bot.tree.command(name="help", description=f"{Elookup('help')} Shows an interactive list of commands and their descriptions.")
async def help_cmd(interaction: discord.Interaction): 
    view = HelpView(interaction.user.id)
    await view.start(interaction)


# --- Plans Command (NEW) ---
class PlanCategorySelectPlans(discord.ui.Select):
    def __init__(self, parent_view: 'PlansView'):
        self.parent_view = parent_view
        options = []
        # Always add Main Overview
        options.append(discord.SelectOption(label=f"{Elookup('plan_category','📜')} Overview / All Plans", value="main", emoji=Elookup('plan_category','📜')))

        if PAID_PLANS_DATA:
             options.append(discord.SelectOption(label=f"{Elookup('paid_plan','💰')} Paid VPS Plans", value="paid", emoji=Elookup('paid_plan','💰')))
        if BOOST_REWARDS_ENABLED and BOOST_REWARD_TIERS:
            options.append(discord.SelectOption(label=f"{Elookup('boost_plan','🚀')} Server Boost Reward Plans", value="boost", emoji=Elookup('boost_plan','🚀')))
        if INVITE_REWARDS_ENABLED_GLOBAL and INVITE_REWARD_TIERS:
            options.append(discord.SelectOption(label=f"{Elookup('invite_plan','💌')} Invite Reward Plans", value="invite", emoji=Elookup('invite_plan','💌')))
        
        current_label = "Overview" 
        if hasattr(parent_view, 'current_category') and parent_view.current_category:
            cat_map = {"main": "Overview", "paid": "Paid Plans", "invite": "Invite Reward Plans", "boost": "Server Boost Plans"}
            current_label = cat_map.get(parent_view.current_category, "Overview")
        
        super().__init__(placeholder=f"Currently Viewing: {current_label} - Select Category...", options=options, min_values=1, max_values=1)

    async def callback(self, interaction: discord.Interaction): # interaction from select
        await interaction.response.defer() 
        await self.parent_view.show_category(interaction, self.values[0])

class PlansView(discord.ui.View):
    def __init__(self, interaction_user_id: int):
        super().__init__(timeout=300) # 5 minutes
        self.interaction_user_id = interaction_user_id
        self.message: Optional[discord.Message] = None
        self.current_category: str = "main" 
        self._update_select_placeholder() 

    def _update_select_placeholder(self):
        for item in list(self.children): 
            if isinstance(item, PlanCategorySelectPlans):
                self.remove_item(item)
        self.add_item(PlanCategorySelectPlans(self)) # Add new select with updated placeholder

    async def interaction_check(self, interaction: discord.Interaction) -> bool: 
        if interaction.user.id != self.interaction_user_id:
            await interaction.response.send_message(f"{Elookup('error')} This plans viewer is not for you! Use `/plans` yourself.", ephemeral=True)
            return False
        return True

    def _format_plan_field(self, plan_data: dict, plan_type: str) -> str:
        # Ensure cpu_cores is used consistently from config for display
        value = (
            f"{Elookup('ram','💾')} RAM: **{plan_data.get('ram_gb','N/A')}GB**\n"
            f"{Elookup('cpu','⚙️')} CPU Cores: **{plan_data.get('cpu_cores','N/A')}**\n"
            f"{Elookup('disk','📀')} Disk: **{plan_data.get('disk_gb','N/A')}GB SSD**\n"
        )
        if plan_type == "paid":
            value += f"💵 Price: **${plan_data.get('price','N/A')}/monthly**\n*To order this plan, please open a support ticket in our Discord server.*"
        elif plan_type == "invite":
            value += f"🫂 Required: **{plan_data.get('invites_required','N/A')}** verified server invites.\n*Claim via `/create` once requirements are met.*"
        elif plan_type == "boost":
            value += f"🚀 Required: **{plan_data.get('server_boosts_required','N/A')}** server boosts (you must be actively boosting this server).\n*Claim via `/create` while boosting.*"
        return value

    def create_embed_for_category(self) -> discord.Embed:
        embed: discord.Embed
        title_prefix = f"{Elookup('plan_category','📜')} Rn Nodes VPS Plans"
        common_footer = "Your Vision, Virtually Realized. | Use dropdown to switch categories."

        if self.current_category == "main":
            embed = discord.Embed(
                title=title_prefix + " - Overview",
                description="Welcome! Below is an overview of our VPS plan categories. Select a specific category from the dropdown menu for detailed information on each plan.",
                color=discord.Color.blurple()
            )
            if bot.user and bot.user.avatar: embed.set_thumbnail(url=bot.user.avatar.url)
            
            has_any_plan = False
            if PAID_PLANS_DATA:
                embed.add_field(name=f"{Elookup('paid_plan','💰')} Paid Plans", value="Explore our competitively priced VPS tiers, offering robust performance and reliability for all your project needs.", inline=False)
                has_any_plan = True
            if BOOST_REWARDS_ENABLED and BOOST_REWARD_TIERS:
                embed.add_field(name=f"{Elookup('boost_plan','🚀')} Server Boost Reward Plans", value="Support our Discord community by boosting the server and get rewarded with a powerful free VPS!", inline=False)
                has_any_plan = True
            if INVITE_REWARDS_ENABLED_GLOBAL and INVITE_REWARD_TIERS:
                embed.add_field(name=f"{Elookup('invite_plan','💌')} Invite Reward Plans", value="Help our community grow by inviting new members! Earn a free VPS based on the number of successful invites.", inline=False)
                has_any_plan = True
            
            if not has_any_plan:
                embed.description = "It seems no VPS plans are currently configured or available. Please check back later or contact an administrator for more information."
            embed.set_footer(text=common_footer)

        elif self.current_category == "paid":
            embed = discord.Embed(title=title_prefix + f" - {Elookup('paid_plan','💰')} Paid Plans", description="Our selection of powerful and affordable paid VPS solutions. All plans feature high-speed SSD storage and reliable infrastructure.", color=discord.Color.green())
            if not PAID_PLANS_DATA: embed.description = "Details for paid plans are currently unavailable. Please contact support or check our website for the latest offerings."
            for plan in PAID_PLANS_DATA:
                embed.add_field(name=f"{plan.get('emoji', Elookup('paid_plan','💰'))} {plan.get('name', 'Paid Plan Tier')}", value=self._format_plan_field(plan, "paid"), inline=True)
            embed.set_footer(text=common_footer)
        
        elif self.current_category == "boost":
            # --- FIX IS HERE ---
            embed = discord.Embed(title=title_prefix + f" - {Elookup('boost_plan','🚀')} Server Boost Rewards", description="Show your support for our community by boosting the Discord server and get rewarded with a fantastic free VPS!", color=discord.Color.magenta()) 
            # --- END OF FIX ---
            if not BOOST_REWARDS_ENABLED or not BOOST_REWARD_TIERS:
                embed.description = "Server Boost reward plans are currently disabled or no tiers are configured. Check back later!"
            else:
                for plan in BOOST_REWARD_TIERS:
                    embed.add_field(name=f"{plan.get('emoji',Elookup('boost_plan','🚀'))} {plan.get('name', 'Boost Reward Tier')}", value=self._format_plan_field(plan, "boost"), inline=True)
            embed.set_footer(text=common_footer)
        
        elif self.current_category == "invite":
            embed = discord.Embed(title=title_prefix + f" - {Elookup('invite_plan','💌')} Invite Rewards", description="Help our community flourish by inviting new members! Earn a free VPS for your efforts based on verified invites.", color=discord.Color.gold())
            if not INVITE_REWARDS_ENABLED_GLOBAL or not INVITE_REWARD_TIERS:
                embed.description = "Invite-based reward plans are currently disabled or no tiers are configured. Stay tuned for updates!"
            else:
                for plan in INVITE_REWARD_TIERS:
                     embed.add_field(name=f"{plan.get('emoji',Elookup('invite_plan','💌'))} {plan.get('name', 'Invite Reward Tier')}", value=self._format_plan_field(plan, "invite"), inline=True)
            embed.set_footer(text=common_footer)
        else: # Should not be reached if select options are controlled
            embed = discord.Embed(title="Error: Unknown Plan Category", description="The selected plan category does not exist or is not recognized.", color=discord.Color.red())
            embed.set_footer(text=common_footer)
        
        return embed

    async def show_category(self, interaction: discord.Interaction, category: str): # interaction is from Select
        self.current_category = category
        self._update_select_placeholder() # This recreates the select dropdown with an updated placeholder
        
        # This method is responsible for generating the embed based on self.current_category.
        # It internally checks BOOST_REWARDS_ENABLED and BOOST_REWARD_TIERS for the 'boost' category.
        embed = self.create_embed_for_category() 
        
        if self.message: # If we have the original /plans message to edit
            try:
                await self.message.edit(embed=embed, view=self)
            except discord.HTTPException as e:
                logger.error(f"PlansView.show_category: HTTPError editing self.message (ID: {self.message.id if self.message else 'Unknown Message ID'}): {e}")
                # Attempt to send a followup to the select interaction if editing the main message fails.
                try:
                    await interaction.followup.send(
                        f"{Elookup('error')} An error occurred while updating the plan display (Code: PVSCEH1). Please try using `/plans` again.", 
                        ephemeral=True
                    )
                except discord.HTTPException as fe:
                    logger.error(f"PlansView.show_category: Failed to send followup error after primary edit failed (Code: PVSCEH2): {fe}")
            except Exception as e:
                logger.error(f"PlansView.show_category: Unexpected error editing self.message (ID: {self.message.id if self.message else 'Unknown Message ID'}): {e}", exc_info=True)
                try:
                    await interaction.followup.send(
                        f"{Elookup('error')} An unexpected error occurred while updating the plan display (Code: PVSCEU1). Please try `/plans` again.", 
                        ephemeral=True
                    )
                except discord.HTTPException as fe:
                     logger.error(f"PlansView.show_category: Failed to send followup for unexpected error (Code: PVSCEU2): {fe}")
        else: 
            # This case means the reference to the original /plans message was somehow lost.
            # This shouldn't happen if view.start() correctly sets self.message.
            logger.warning(
                f"PlansView.show_category: self.message was None when trying to show category '{category}'. "
                f"This indicates a potential issue with message state management or that the original message was deleted. "
                f"Interaction ID: {interaction.id}, User: {interaction.user} ({interaction.user.id})"
            )
            # The interaction here is from the Select component. Its original response was defer().
            # We can only send a followup. This followup will be a new message, not an edit of the (lost) original.
            try:
                await interaction.followup.send(
                    f"{Elookup('error')} There was an internal issue updating the display because the original message context was lost (Code: PVSCSMN). Please try the `/plans` command again.", 
                    ephemeral=True
                )
            except discord.HTTPException as e:
                logger.error(f"PlansView.show_category: Failed to send followup when self.message was None (Code: PVSCSMNF): {e}")


    async def start(self, interaction: discord.Interaction): # interaction is the original /plans command
        self.current_category = "main" 
        self._update_select_placeholder() 
        embed = self.create_embed_for_category()
        await interaction.response.send_message(embed=embed, view=self, ephemeral=False) # Non-ephemeral for plans
        try:
            self.message = await interaction.original_response()
        except discord.HTTPException as e:
            logger.error(f"Failed to get original_response for PlansView after send_message: {e}")


    async def on_timeout(self):
        if self.message:
            try:
                for item in self.children: item.disabled = True
                timeout_embed = self.create_embed_for_category() # Get last viewed category
                timeout_embed.title += " (Session Timed Out)"
                timeout_embed.description = f"{Elookup('loading')} This interactive plan viewer session has timed out. Please use `/plans` again if you wish to view them."
                if timeout_embed.footer:
                    timeout_embed.footer.text = "Session timed out. | " + (timeout_embed.footer.text or "")
                else:
                    timeout_embed.set_footer(text="Session timed out.")
                
                await self.message.edit(embed=timeout_embed, view=self)
            except discord.HTTPException as e:
                logger.warning(f"HTTPException during PlansView on_timeout: {e}")
            except Exception as e:
                logger.error(f"Unexpected error during PlansView on_timeout: {e}", exc_info=True)

@bot.tree.command(name="plans", description=f"{Elookup('plan_category')} View available VPS plans (Paid, Boost, Invite Rewards).")
async def plans_cmd(interaction: discord.Interaction): 
    view = PlansView(interaction.user.id)
    await view.start(interaction)


@bot.tree.command(name="generatepassword", description=f"{Elookup('password')} Generates a strong, compliant password.") 
@app_commands.describe(length="The desired length of the password (min 8, max 50). Default is 16.")
async def generate_password_cmd(interaction: discord.Interaction, length: app_commands.Range[int, 8, 50] = 16): 
    password = generate_compliant_password(length)
    embed = discord.Embed(title=f"{Elookup('password')} Secure Generated Password", description=f"A strong password of length `{length}` has been generated for you:", color=discord.Color.light_grey())
    embed.add_field(name="Your New Password:", value=f"||```{password}```||", inline=False) # Spoiler for privacy
    embed.set_footer(text="Keep this password secret and safe! Do not share it unnecessarily.")
    await interaction.response.send_message(embed=embed, ephemeral=True) # Ephemeral for privacy

# --- /nodes Command ---
def format_size(size_bytes):
    if not isinstance(size_bytes, (int, float)) or size_bytes < 0: return "N/A"
    if size_bytes == 0: return "0 B"
    names = ["B", "KB", "MB", "GB", "TB", "PB"] # Added PB
    i = 0
    # Prefer GiB, MiB for display if base-2 is implied by panel (Convoy often uses base-2 for RAM/Disk display)
    # For simplicity, using standard KB, MB, GB here. If panel reports in MiB/GiB, adjust.
    while size_bytes >= 1024 and i < len(names) - 1:
        size_bytes /= 1024.0
        i += 1
    return f"{size_bytes:.2f} {names[i]}"

def create_progress_bar(percentage_val, length=10):
    # Ensure percentage_val is a number
    if not isinstance(percentage_val, (int, float)):
        try: 
            percentage_val = float(str(percentage_val).rstrip('%'))
        except ValueError: 
            return Elookup("unknown_status","▫️") * length # Default for invalid input
            
    percentage = max(0.0, min(100.0, percentage_val))
    filled_count = int(round((percentage / 100.0) * length))
    
    # Color based on usage
    if percentage < 50: filled_char = Elookup("bar_green","🟩")
    elif percentage < 80: filled_char = Elookup("bar_yellow","🟨")
    else: filled_char = Elookup("bar_red","🟥")
    empty_char = Elookup("bar_empty","⬜")
    
    return filled_char * filled_count + empty_char * (length - filled_count)

def calculate_percentage(part, total):
    if not all(isinstance(x, (int, float)) for x in [part, total]) or total == 0: return 0.0
    return round((float(part) / float(total)) * 100.0, 1)

async def check_node_connectivity(ip: str, port: int, node_identifier: str) -> dict:
    result = { "status": "Offline", "emoji": Elookup("offline","❌"), "latency": None }
    try:
        start_time = time.monotonic()
        # Use asyncio.open_connection for non-blocking socket connection
        reader, writer = await asyncio.wait_for(asyncio.open_connection(ip, port), timeout=2.0)
        latency = int((time.monotonic() - start_time) * 1000)
        writer.close()
        await writer.wait_closed() # Ensure connection is closed
        result.update({"status": "Online", "latency": latency, "emoji": Elookup("online","✅")})
        if latency > 500: result["emoji"] = Elookup("latency_high","🟧") # High latency
        elif latency > 200: result["emoji"] = Elookup("latency_medium","🟨") # Medium latency
    except asyncio.TimeoutError: result.update({"status": "Timeout", "emoji": Elookup("timeout","⏱️")})
    except ConnectionRefusedError: result.update({"status": "Connection Refused", "emoji": Elookup("conn_refused","🚫")})
    except socket.gaierror: result.update({"status": "DNS Error", "emoji": Elookup("dns_error","🌐")}) # For FQDN resolving issues if passed here
    except OSError as e: 
        result.update({"status": "Network Error", "emoji": Elookup("net_error","🔌")})
        logger.warning(f"Node connectivity check OS Error for {node_identifier} ({ip}:{port}): {e}")
    except Exception as e: 
        result.update({"status": "Check Error", "emoji": Elookup("error","⚠️")})
        logger.error(f"Unexpected Node connectivity check Error for {node_identifier} ({ip}:{port}): {e}", exc_info=True)
    return result


@bot.tree.command(name="nodes", description="🌐 Shows detailed status of panel nodes with resource allocation.")
async def nodes(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=False)
    node_data = await make_api_request('GET', '/nodes', 'application', interaction)
    embed = discord.Embed(title="📊 Panel Node Dashboard", color=discord.Color.dark_blue())
    
    if node_data and isinstance(node_data.get('data'), list):
        api_nodes = node_data['data']
        if not api_nodes:
            embed.description = "🍃 No node information is currently available from the API."
        else:
            # Fetch all locations to get location names
            locations_data = await make_api_request('GET', '/locations', 'application', interaction)
            location_map = {}
            if locations_data and isinstance(locations_data.get('data'), list):
                for location in locations_data['data']:
                    location_map[location.get('id')] = location.get('short', location.get('long', 'Unknown'))
            
            embed.description = f"📡 Displaying status for **{len(api_nodes)}** nodes\n⏰ Last refreshed: {discord.utils.format_dt(datetime.now(), 'R')}"
            
            check_tasks = []
            node_map = {}  # identifier -> {node_info, status_info}
            
            for node in api_nodes:
                node_id = node.get('id', 'N/A')
                node_name = node.get('name', f'Unknown Node {node_id}')
                node_location_id = node.get('location_id')
                node_location = location_map.get(node_location_id, f"Location ID: {node_location_id}")
                node_cluster = node.get('cluster', 'N/A')
                
                # Additional node info from API
                node_fqdn = node.get('fqdn', 'N/A')
                node_port = node.get('port', 8006)
                node_memory = node.get('memory', 0)
                node_memory_allocated = node.get('memory_allocated', 0)
                node_memory_overallocate = node.get('memory_overallocate', 0)
                node_disk = node.get('disk', 0)
                node_disk_allocated = node.get('disk_allocated', 0)
                node_disk_overallocate = node.get('disk_overallocate', 0)
                node_vm_storage = node.get('vm_storage', 'N/A')
                node_backup_storage = node.get('backup_storage', 'N/A') 
                node_iso_storage = node.get('iso_storage', 'N/A')
                node_network = node.get('network', 'N/A')
                node_verify_tls = node.get('verify_tls', True)
                node_servers_count = node.get('servers_count', 0)
                
                identifier = f"{node_name} (ID: {node_id})"
                
                # Get configured IP for the node without displaying it
                if node_name == "US-1":
                    node_ip = "104.192.1.74"
                    logger.info(f"Using hardcoded IP {node_ip} for node '{node_name}'")
                else:
                    # 2. If not a specific override, try FQDN lookup (non-blocking)
                    if node_fqdn and node_fqdn != "error" and node_fqdn != "N/A" and '.' in node_fqdn:
                        try:
                            # Use asyncio's getaddrinfo for non-blocking DNS lookup
                            addr_info = await asyncio.get_event_loop().getaddrinfo(node_fqdn, None, family=socket.AF_INET)
                            if addr_info:
                               node_ip = addr_info[0][4][0] # Get the first IPv4 address
                               logger.debug(f"Resolved FQDN {node_fqdn} to {node_ip} for node '{node_name}'")
                        except socket.gaierror:
                            logger.warning(f"DNS lookup failed for FQDN: {node_fqdn} (Node: {node_name})")
                            node_ip = None # Ensure node_ip is None if lookup fails
                        except Exception as e:
                             logger.error(f"Unexpected error during DNS lookup for {node_fqdn} (Node: {node_name}): {e}")
                             node_ip = None # Ensure node_ip is None on error
                    
                    # Check if FQDN might be an IP directly (if DNS didn't resolve or wasn't attempted)
                    if not node_ip and node_fqdn and node_fqdn != "error" and node_fqdn != "N/A":
                         try:
                             socket.inet_aton(node_fqdn) # Check if it's a valid IPv4
                             node_ip = node_fqdn
                             logger.debug(f"Using FQDN field '{node_fqdn}' directly as IP for node '{node_name}'")
                         except socket.error:
                             # FQDN exists but is not a valid domain (per lookup) or IP
                             logger.warning(f"FQDN field '{node_fqdn}' for node '{node_name}' is not a resolvable domain or valid IP.")
                             node_ip = None # Ensure node_ip is None

                    # 3. If FQDN didn't yield an IP, try the NODE_IPS dictionary fallback
                    if not node_ip:
                        # Check NODE_IPS using node_name first, then node_id
                        ip_from_map = NODE_IPS.get(node_name) or NODE_IPS.get(str(node_id))
                        if ip_from_map:
                            node_ip = ip_from_map
                            logger.debug(f"Using IP {node_ip} from NODE_IPS map for node '{node_name}'")
                
                # Calculate usage percentages
                memory_usage_pct = calculate_percentage(node_memory_allocated, node_memory)
                disk_usage_pct = calculate_percentage(node_disk_allocated, node_disk)
                
                # Store all node info
                node_map[identifier] = {
                    "node_info": {
                        "location": node_location,
                        "cluster": node_cluster,
                        "fqdn": node_fqdn,
                        "port": node_port,
                        "memory": format_size(node_memory),
                        "memory_allocated": format_size(node_memory_allocated),
                        "memory_usage": memory_usage_pct,
                        "memory_overallocate": f"{node_memory_overallocate}%",
                        "disk": format_size(node_disk),
                        "disk_allocated": format_size(node_disk_allocated),
                        "disk_usage": disk_usage_pct,
                        "disk_overallocate": f"{node_disk_overallocate}%",
                        "vm_storage": node_vm_storage,
                        "backup_storage": node_backup_storage,
                        "iso_storage": node_iso_storage,
                        "network": node_network,
                        "verify_tls": "Yes" if node_verify_tls else "No",
                        "servers_count": node_servers_count
                    },
                    "status_info": {
                        "status": "Checking...",
                        "emoji": "⏳",
                        "latency": None,
                        "has_ip": node_ip is not None
                    }
                }
                
                # Schedule connectivity check without exposing IP
                if node_ip:
                    check_tasks.append(asyncio.create_task(
                        check_node_connectivity(node_ip, node_port, identifier),
                        name=identifier
                    ))
                else:
                    node_map[identifier]["status_info"]["status"] = "IP Not Configured"
                    node_map[identifier]["status_info"]["emoji"] = "❓"
            
            # Wait for all connectivity checks to complete
            if check_tasks:
                done, pending = await asyncio.wait(check_tasks, timeout=12)
                
                for task in done:
                    task_id = task.get_name()
                    try:
                        result = task.result()
                        node_map[task_id]["status_info"]["status"] = result["status"]
                        node_map[task_id]["status_info"]["emoji"] = result["emoji"]
                        node_map[task_id]["status_info"]["latency"] = result["latency"]
                    except Exception as e:
                        logger.error(f"Error checking node {task_id}: {e}")
                        node_map[task_id]["status_info"]["status"] = "Check Error"
                        node_map[task_id]["status_info"]["emoji"] = "⚠️"
                
                for task in pending:
                    task_id = task.get_name()
                    task.cancel()
                    node_map[task_id]["status_info"]["status"] = "Check Timed Out"
                    node_map[task_id]["status_info"]["emoji"] = "⏱️"
            
            # Organize nodes by status
            online_nodes = []
            offline_nodes = []
            other_nodes = []
            
            for identifier, data in node_map.items():
                status = data["status_info"]["status"]
                if status == "Online":
                    online_nodes.append((identifier, data))
                elif status == "Offline":
                    offline_nodes.append((identifier, data))
                else:
                    other_nodes.append((identifier, data))
            
            # Sort nodes within each category
            online_nodes.sort(key=lambda x: x[0])
            offline_nodes.sort(key=lambda x: x[0])
            other_nodes.sort(key=lambda x: x[0])
            
            # Add sorted nodes to embed
            nodes_to_display = online_nodes + offline_nodes + other_nodes
            
            for identifier, data in nodes_to_display:
                node_info = data["node_info"]
                status_info = data["status_info"]
                
                status_line = f"{status_info['emoji']} **{status_info['status']}**"
                if status_info['latency'] is not None:
                    status_line += f" (Response: {status_info['latency']}ms)"
                
                # Create progress bars for memory and disk usage
                memory_bar = create_progress_bar(node_info['memory_usage'])
                disk_bar = create_progress_bar(node_info['disk_usage'])
                
                # Format node details in a clean way
                value = (
                    f"📡 **Status:** {status_line}\n"
                    f"📍 **Location:** {node_info['location']} (Cluster: {node_info['cluster']})\n"
                    f"🖥️ **FQDN:** `{node_info['fqdn']}` (Port: {node_info['port']})\n"
                    f"💾 **Memory:** {memory_bar}\n"
                    f"   {node_info['memory_allocated']} / {node_info['memory']} ({node_info['memory_usage']}%) used\n"
                    f"💿 **Disk:** {disk_bar}\n"
                    f"   {node_info['disk_allocated']} / {node_info['disk']} ({node_info['disk_usage']}%) used\n"
                    f"🔢 **Servers:** {node_info['servers_count']}\n"
                    f"🗄️ **Storage:** VM: {node_info['vm_storage']}, Backup: {node_info['backup_storage']}, ISO: {node_info['iso_storage']}\n"
                    f"🌐 **Network:** {node_info['network']}"
                )
                
                embed.add_field(name=identifier, value=value, inline=False)
    
    elif node_data is not None:
        embed.description = f"⚠️ Could not parse node information.\n```\n{str(node_data)[:500]}\n```"
    
    # Add footer with summary
    if 'api_nodes' in locals() and api_nodes:
        online_count = sum(1 for _, data in node_map.items() if data["status_info"]["status"] == "Online")
        embed.set_footer(text=f"✅ {online_count}/{len(api_nodes)} nodes online | Server time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    await interaction.followup.send(embed=embed)

# Modified check_node_connectivity function with updated port parameter
async def check_node_connectivity(ip: str, port: int, node_identifier: str) -> dict:
    result = {
        "status": "Offline",
        "emoji": Elookup("offline", "❌"),
        "latency": None
    }
    
    try:
        start_time = time.monotonic()
        # Use asyncio.open_connection for non-blocking socket connection
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(ip, port),
            timeout=5
        )
        
        # Calculate latency
        latency = int((time.monotonic() - start_time) * 1000)
        result["latency"] = latency
        
        # Close the connection
        writer.close()
        await writer.wait_closed()
        
        result["status"] = "Online"
        
        # Add emoji based on latency
        if latency < 50:
            result["emoji"] = Elookup("online", "✅")  # Very fast
        elif latency < 150:
            result["emoji"] = Elookup("online", "🟢")  # Good
        elif latency < 300:
            result["emoji"] = Elookup("latency_medium", "🟡")  # Moderate
        elif latency < 500:
            result["emoji"] = Elookup("latency_high", "🟠")  # Slow
        else:
            result["emoji"] = Elookup("latency_critical", "🔴")  # Very slow
            
    except asyncio.TimeoutError:
        result["status"] = "Timeout"
        result["emoji"] = Elookup("timeout", "⏱️")
    except ConnectionRefusedError:
        result["status"] = "Connection Refused"
        result["emoji"] = Elookup("conn_refused", "🚫")
    except socket.gaierror:
        result["status"] = "DNS Error"
        result["emoji"] = Elookup("dns_error", "🌐")
    except OSError as e:
        result["status"] = "Network Error"
        result["emoji"] = Elookup("net_error", "🔌")
        logger.warning(f"Node connectivity check OS Error for {node_identifier} ({ip}:{port}): {e}")
    except Exception as e:
        result["status"] = f"Error: {type(e).__name__}"
        result["emoji"] = Elookup("error", "⚠️")
        logger.error(f"Error checking node {node_identifier}: {e}")
    
    return result

@bot.tree.command(name="links", description=f"{Elookup('links')} Shows a list of useful Rn Nodes links.")
async def links_cmd(interaction: discord.Interaction): 
    embed = discord.Embed(
        title=f"{Elookup('links','🔗')} Rn Nodes - Useful Links", 
        description="Here are some important links to help you navigate the Rn Nodes ecosystem:",
        color=discord.Color.orange()
    )
    embed.add_field(name="🚀 Client & VPS Panel", value=f"Manage your services: <{PANEL_BASE_URL}>", inline=False)
    embed.add_field(name="✨ Rn Nodes Official Website", value="Find out more about us: https://vm.rnnodes.qzz.io", inline=False)
    
    if interaction.guild and interaction.guild.vanity_url:
         embed.add_field(name="💬 Our Discord Community", value=f"Join the conversation: {interaction.guild.vanity_url}", inline=False)
    elif interaction.guild: # Fallback if no vanity, try to create an instant invite (might fail if no perms)
        try:
            invite = await interaction.guild.text_channels[0].create_invite(max_age=0, max_uses=0, unique=False, reason="For /links command")
            embed.add_field(name="💬 Our Discord Community", value=f"Join the conversation: {invite.url}", inline=False)
        except (discord.Forbidden, discord.HTTPException):
            embed.add_field(name="💬 Our Discord Community", value="Ask a staff member for a current invite link!", inline=False)
            
    embed.add_field(name="📚 VPS Plans Overview", value="See all available plans: Use `/plans` command", inline=False)
    embed.add_field(name="💡 Need Help?", value="Use `/help` or open a support ticket.", inline=False)

    if bot.user and bot.user.avatar:
        embed.set_thumbnail(url=bot.user.avatar.url)
    embed.set_footer(text="Rn Nodes - Your Vision, Virtually Realized.")
    await interaction.response.send_message(embed=embed, ephemeral=False)


# --- Bot Events ---
@bot.event
async def on_ready():
    logger.info(f"Bot Logged In: {bot.user.name} (ID: {bot.user.id})")
    logger.info(f"Discord.py Version: {discord.__version__}")
    logger.info(f"Connected to {len(bot.guilds)} guild(s).")
    logger.info(f"Application API Key Loaded: {'Yes' if CONVOY_APP_API_KEY else 'NO'}")
    logger.info(f"Client API Key Loaded: {'Yes' if CONVOY_CLIENT_API_KEY else 'NO'}")
    logger.info(f"Panel Base URL: {PANEL_BASE_URL}")
    logger.info(f"VPS Creator Role ID: {VPS_CREATOR_ROLE_ID}")
    logger.info(f"Bot Owner User ID: {BOT_OWNER_USER_ID}")
    logger.info(f"VPS Log Channel ID: {VPS_LOG_CHANNEL_ID if VPS_LOG_CHANNEL_ID else 'Not Set'}")
    logger.info(f"Admin Approval Channel ID: {ADMIN_VPS_APPROVAL_CHANNEL_ID if ADMIN_VPS_APPROVAL_CHANNEL_ID else 'Not Set'}")
    
    activity_conf = config.get('bot_activity', {})
    activity_name = activity_conf.get("name", "over Rn Nodes") # Default name
    activity_type_str = activity_conf.get("type", "watching").lower()
    stream_url = activity_conf.get("stream_url", "https://www.twitch.tv/rnnodes") # Example

    activity_map = {
        "playing": discord.ActivityType.playing, "streaming": discord.ActivityType.streaming, 
        "listening": discord.ActivityType.listening, "watching": discord.ActivityType.watching,
        "competing": discord.ActivityType.competing
    }
    selected_activity_type = activity_map.get(activity_type_str, discord.ActivityType.watching)
    
    activity_args = {"name": activity_name, "type": selected_activity_type}
    if selected_activity_type == discord.ActivityType.streaming: 
        activity_args["url"] = stream_url
        
    try:
        await bot.change_presence(activity=discord.Activity(**activity_args))
        logger.info(f"Bot presence set to: {selected_activity_type.name.capitalize()} {activity_name}")
    except Exception as e:
        logger.error(f"Failed to set bot presence: {e}")

@bot.event
async def on_member_join(member: discord.Member):
    if not member.guild or member.bot: return # Ignore bots or DMs
    
    # logger.debug(f"Member joined: {member.name} ({member.id}) in guild {member.guild.name} ({member.guild.id}). Waiting {INVITE_CHECK_DELAY_SECONDS}s for invite data.")
    await asyncio.sleep(INVITE_CHECK_DELAY_SECONDS) 
    
    try:
        current_guild_invites = await member.guild.invites()
    except discord.Forbidden: 
        # logger.warning(f"No permission to fetch invites in guild {member.guild.name} ({member.guild.id}). Cannot track invite for {member.name}.")
        return
    except discord.HTTPException as e: 
        logger.error(f"HTTPException while fetching invites for guild {member.guild.name} ({member.guild.id}): {e}")
        return

    cached_guild_invites_map = guild_invite_cache.get(member.guild.id, {})
    found_inviter_user: Optional[discord.User] = None

    for new_invite_obj in current_guild_invites:
        if new_invite_obj.code is None or new_invite_obj.uses is None or new_invite_obj.inviter is None: 
            continue # Skip incomplete invite data
        
        # If invite code wasn't cached OR its use count increased
        if new_invite_obj.code not in cached_guild_invites_map or \
           new_invite_obj.uses > cached_guild_invites_map.get(new_invite_obj.code, -1): # Default -1 for codes not in cache
            
            if found_inviter_user is None: # First potential inviter found
                found_inviter_user = new_invite_obj.inviter
            else: # Ambiguity: multiple invites could account for the join
                logger.warning(f"Ambiguous inviter detection for new member {member.name} in guild {member.guild.name}. Multiple new/updated invites.")
                found_inviter_user = None # Reset to None due to ambiguity
                break 
    
    if found_inviter_user and found_inviter_user.id != member.id: # Ensure not self-invite
        increment_invite_count(member.guild.id, found_inviter_user.id)
        logger.info(f"Attributed join of {member.name} ({member.id}) in guild {member.guild.name} to inviter {found_inviter_user.name} ({found_inviter_user.id}).")
    elif found_inviter_user and found_inviter_user.id == member.id:
        logger.info(f"Member {member.name} joined with their own invite in guild {member.guild.name}. Not counted.")
    # else: (no inviter found or ambiguous)
        # logger.debug(f"Could not determine a unique inviter for {member.name} in {member.guild.name}.")

    # Update cache for this guild with the latest invite uses
    guild_invite_cache[member.guild.id] = {inv.code: inv.uses for inv in current_guild_invites if inv.code and inv.uses is not None}


@bot.event
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    original_error = getattr(error, 'original', error)
    command_name = interaction.command.name if interaction.command else "an unknown command"

    if isinstance(original_error, app_commands.CommandNotFound): 
        # Discord usually handles this, but log it for awareness if it reaches here
        logger.warning(f"CommandNotFound error for '/{command_name}' by {interaction.user} - Discord should handle this.")
        return 
        
    if isinstance(original_error, app_commands.CheckFailure): 
        # Custom check predicates (is_vps_creator, is_bot_owner) already send their own messages.
        logger.warning(f"CheckFailure for user {interaction.user} on command '/{command_name}'. Custom message should have been sent.")
        return 
        
    if isinstance(original_error, app_commands.CommandOnCooldown):
        retry_after_formatted = f"{original_error.retry_after:.1f} seconds"
        return await send_error_embed(interaction, "Command on Cooldown", f"This command is on cooldown. Please try again in **{retry_after_formatted}**.", ephemeral=True)
        
    if isinstance(original_error, app_commands.BotMissingPermissions):
        missing_perms_str = ", ".join(f"`{perm}`" for perm in original_error.missing_permissions)
        logger.error(f"BotMissingPermissions for '/{command_name}': needs {missing_perms_str}")
        return await send_error_embed(interaction, "Bot Permission Error", f"I'm missing the following permission(s) to execute this command: {missing_perms_str}. Please ask a server administrator to grant them.", ephemeral=True)

    if isinstance(original_error, app_commands.MissingPermissions):
        missing_perms_str = ", ".join(f"`{perm}`" for perm in original_error.missing_permissions)
        logger.warning(f"User {interaction.user} lacks permissions for '/{command_name}': needs {missing_perms_str}")
        return await send_error_embed(interaction, "Permission Denied", f"You are missing the following permission(s) to use this command: {missing_perms_str}.", ephemeral=True)
        
    # Log other errors with more detail
    logger.error(f"Unhandled error in application command '/{command_name}' invoked by {interaction.user} (ID: {interaction.user.id}):", exc_info=original_error)
    
    # Send a generic error message to the user if no response has been sent yet
    if not interaction.response.is_done():
        await send_error_embed(interaction, "Command Execution Error", "An unexpected internal error occurred while trying to execute this command. The development team has been (metaphorically) notified. Please try again later.", ephemeral=True)
    else: # If already responded, try a followup
        try:
            await interaction.followup.send(f"{Elookup('error')} An unexpected internal error occurred. If the issue persists, please report it.",ephemeral=True)
        except discord.HTTPException: # Followup also failed
            logger.error(f"Failed to send followup error message for '/{command_name}'.")


# --- Sync Command (Bot Owner Only) ---
@bot.tree.command(name="sync", description=f"{Elookup('sync')} Syncs application (slash) commands with Discord (Bot Owner).")
@is_bot_owner()
@app_commands.choices(scope=[
    app_commands.Choice(name="Current Guild (For Testing/Development)", value="guild"),
    app_commands.Choice(name="Global (For Production - Takes Time)", value="global"),
    app_commands.Choice(name="Clear & Sync Guild (Dev Reset)", value="clear_guild_sync")
])
async def sync_cmd(interaction: discord.Interaction, scope: app_commands.Choice[str]): 
    await interaction.response.defer(ephemeral=True)
    synced_count = 0
    location_msg = ""
    
    try:
        if scope.value == 'guild':
            if not interaction.guild: 
                return await interaction.followup.send("❌ Guild sync must be performed within a server.", ephemeral=True)
            logger.info(f"Initiating command sync to current guild: {interaction.guild.name} ({interaction.guild.id})")
            # bot.tree.copy_global_to(guild=interaction.guild) # This ensures guild gets global commands too
            synced_list = await bot.tree.sync(guild=interaction.guild)
            synced_count = len(synced_list)
            location_msg = f"to the current guild **{interaction.guild.name}**"
            logger.info(f"Synced {synced_count} commands to guild {interaction.guild.id}.")

        elif scope.value == 'global':
            logger.info("Initiating global command sync...")
            synced_list = await bot.tree.sync() # Sync global commands
            synced_count = len(synced_list)
            location_msg = "globally"
            logger.info(f"Synced {synced_count} commands globally. Changes may take up to an hour to propagate everywhere.")

        elif scope.value == 'clear_guild_sync':
            if not interaction.guild:
                return await interaction.followup.send("❌ Guild clear & sync must be performed within a server.", ephemeral=True)
            logger.info(f"Initiating CLEAR and then SYNC for guild: {interaction.guild.name} ({interaction.guild.id})")
            bot.tree.clear_commands(guild=interaction.guild) # Clear guild-specific commands
            await bot.tree.sync(guild=interaction.guild) # Sync (will be empty if no guild-specific defined)
            # Then copy global and sync again to effectively refresh
            # bot.tree.copy_global_to(guild=interaction.guild)
            synced_list = await bot.tree.sync(guild=interaction.guild)
            synced_count = len(synced_list)
            location_msg = f"by clearing and re-syncing for current guild **{interaction.guild.name}**"
            logger.info(f"Cleared and re-synced {synced_count} commands for guild {interaction.guild.id}.")
        
        await send_success_embed(interaction, "Command Synchronization Complete", f"Successfully synced **{synced_count}** command(s) {location_msg}. Global changes might take up to an hour to appear.", ephemeral=True)

    except discord.HTTPException as e:
        logger.error(f"HTTPException during command sync ({scope.value}): {e}", exc_info=True)
        await send_error_embed(interaction, "Sync Failed (HTTP Error)", f"An HTTP error occurred while trying to sync commands {location_msg}:\n```py\n{type(e).__name__}: {e}\n```", ephemeral=True)
    except app_commands.CommandSyncFailure as e:
        logger.error(f"CommandSyncFailure during command sync ({scope.value}): {e}", exc_info=True)
        await send_error_embed(interaction, "Sync Failed (Sync Failure)", f"A command sync failure occurred for {location_msg}:\n```py\n{e}\n```", ephemeral=True)
    except Exception as e:
        logger.error(f"Unexpected error during command sync ({scope.value}): {e}", exc_info=True)
        await send_error_embed(interaction, "Sync Failed (Unexpected Error)", f"An unexpected error occurred while syncing {location_msg}:\n```py\n{type(e).__name__}: {e}\n```", ephemeral=True)

# --- Run Bot ---
if __name__ == "__main__":
    if not DISCORD_TOKEN:
        logger.critical("FATAL: Discord token (DISCORD_TOKEN) not found in environment or configuration.")
        exit(1)
    try:
        logger.info("Rn Nodes Bot is preparing to launch...")
        bot.run(DISCORD_TOKEN, log_handler=None) # Use basicConfig for logging
    except discord.LoginFailure:
        logger.critical("FATAL: Discord login failed. Please check if the DISCORD_TOKEN is correct and valid.")
    except discord.PrivilegedIntentsRequired as e:
        logger.critical(f"FATAL: Privileged Intents error. The bot is missing required intents: {e.args}. Please ensure 'Server Members Intent' and 'Message Content Intent' (if needed for prefix commands, though this bot uses slash) are enabled in your bot's application page on the Discord Developer Portal.")
    except Exception as e:
        logger.critical(f"FATAL: An unexpected error occurred while trying to run the bot: {e}", exc_info=True)