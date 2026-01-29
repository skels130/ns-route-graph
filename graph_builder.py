import asyncio
import logging
import re
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from models import (
    CytoscapeElement,
    EdgeData,
    NodeData,
    NSAutoAttendantResponse,
    NSPhoneNumber,
)
from ns_client import NSClient
from utils import format_phone_number, generate_portal_link

logger = logging.getLogger(__name__)


class GraphBuilder:
    def __init__(self, client: NSClient, domain: str):
        self.client = client
        self.domain = domain
        self.users_map: Dict[str, Any] = {}
        self.timeframes_map: Dict[str, Any] = {}

        self.rules_cache: Dict[str, List[Any]] = {}
        self.queue_agents_cache: Dict[str, List[Any]] = {}
        self.aa_prompts_cache: Dict[str, Any] = {}

    async def build(self) -> List[CytoscapeElement]:
        # 1. Pre-fetch Global Data
        logger.info(f"Fetching global data for domain {self.domain}...")
        await self._fetch_global_data()

        # 2. Identify Roots (DIDs)
        logger.info(f"Fetching DIDs for domain {self.domain}...")
        dids = await self.client.get_dids(self.domain)
        logger.info(f"Found {len(dids) if dids else 0} DIDs.")

        # We use a dict to deduplicate elements by ID
        elements_map: Dict[str, CytoscapeElement] = {}

        for did_obj in dids:
            logger.debug(f"Processing call flow for DID: {did_obj.phonenumber}")
            path_elements = await self._process_did_path(did_obj)

            for el in path_elements:
                if isinstance(el.data, NodeData):
                    el_id = el.data.id
                elif isinstance(el.data, EdgeData):
                    el_id = el.data.id or f"{el.data.source}_{el.data.target}"

                if el_id not in elements_map:
                    elements_map[el_id] = el

        return list(elements_map.values())

    async def _fetch_global_data(self):
        results = await asyncio.gather(
            self.client.get_users(self.domain),
            self.client.get_domain_timeframes(self.domain),
            return_exceptions=True,
        )

        users = results[0]
        if isinstance(users, list):
            self.users_map = {u.user: u for u in users}
            logger.debug(f"Cached {len(self.users_map)} users.")
        else:
            logger.warning(f"Failed to fetch users or none found: {users}")

        timeframes = results[1]
        if isinstance(timeframes, list):
            self.timeframes_map = {t.frame: t for t in timeframes}
            logger.debug(f"Cached {len(self.timeframes_map)} timeframes.")

    def _safe_id(self, val: str) -> str:
        return val.replace(":", "_").replace("@", "_").replace(".", "_")

    def _get_type_and_name(
        self, target: Union[str, int]
    ) -> Tuple[str, str, Optional[str]]:
        target = str(target)

        # New pattern: <phone>_callqueue_<user>
        m_queue = re.match(r"^\d+_callqueue_(\w+)$", target)
        if m_queue:
            return "call_queue", m_queue.group(1), f"user_{m_queue.group(1)}"

        # New pattern: <phone>_attendant_<user>
        m_att = re.match(r"^\d+_attendant_(\w+)$", target)
        if m_att:
            return "user", m_att.group(1), None

        # New pattern: <phone>_pstn_<phone>
        m_pstn = re.match(r"^\d+_pstn_(\d+)$", target)
        if m_pstn:
            return "offnet", m_pstn.group(1), None

        if "Prompt" in target or "Announce" in target:
            return "auto_attendant", target, None
        if "vmail_" in target:
            return "voicemail", target, None
        if "queue_" in target:
            return "call_queue", target.replace("queue_", ""), None
        if "user_" in target:
            u_name = target.replace("user_", "")
            if u_name.endswith(f"@{self.domain}"):
                u_name = u_name.split("@")[0]
            return "user", u_name, None
        if "phone_" in target:
            return "device", target.replace("phone_", ""), None
        if target in self.users_map:
        if re.match(r"^1?\d{10}$", target):
            return "offnet", target, None



        if target.lower() == "hangup":
            return "hangup", "Hangup", None

        return "other", target, None

    async def _process_did_path(self, did_obj: NSPhoneNumber) -> List[CytoscapeElement]:
        elements: List[CytoscapeElement] = []
        visited: Set[str] = set()

        # Queue item: (source_id, target_name, target_type, edge_label, extra_edge_data, should_expand, parent_hint)
        queue: List[Tuple[str, str, str, str, Dict[str, Any], bool, Optional[str]]] = []

        did = did_obj.phonenumber
        dest = did_obj.dest

        if not dest:
            logger.warning(f"DID {did} has no destination.")
            return []

        # 1. Create Ingress Node
        root_id = self._safe_id(f"did_{did}")
        formatted_did = format_phone_number(did)

        elements.append(
            CytoscapeElement(
                data=NodeData(
                    id=root_id,
                    label=f"Phone Number: {formatted_did}",
                    type="ingress",
                    bg="#E0E0E0",
                    link=generate_portal_link(self.domain, "ingress", did),
                    details={"Destination": dest, "Application": did_obj.application},
                )
            )
        )

        visited.add(root_id)

        initial_type, initial_name, initial_parent = self._get_type_and_name(dest)
        queue.append(
            (
                root_id,
                initial_name,
                initial_type,
                "Destination",
                {},
                True,
                initial_parent,
            )
        )

        while queue:
            (
                source_id,
                target_name,
                target_type_hint,
                edge_label,
                extra_data,
                should_expand,
                parent_hint,
            ) = queue.pop(0)

            raw_node_id = f"{target_type_hint}_{target_name}"
            node_id = self._safe_id(raw_node_id)

            edge_id = self._safe_id(f"edge_{source_id}_{node_id}")

            edge_data_args = {
                "id": edge_id,
                "source": self._safe_id(source_id),
                "target": node_id,
                "label": edge_label,
                **extra_data,
            }

            elements.append(CytoscapeElement(data=EdgeData(**edge_data_args)))

            if node_id in visited:
                continue
            visited.add(node_id)

            node_label = target_name
            bg_color = "#ADD8E6"  # Default User Blue
            node_link: Optional[str] = generate_portal_link(
                self.domain, target_type_hint, target_name
            )
            node_parent = None
            if parent_hint:
                node_parent = self._safe_id(parent_hint)

            async def get_aa_response(owner, prompt):
                cache_key = f"{owner}:{prompt}"
                if cache_key in self.aa_prompts_cache:
                    return self.aa_prompts_cache[cache_key]
                resp = await self.client.get_auto_attendant_prompts(
                    self.domain, owner, prompt
                )
                self.aa_prompts_cache[cache_key] = resp
                return resp

            node_details = {}

            if target_type_hint == "user":
                user_details = self.users_map.get(target_name)
                if user_details:
                    fname = user_details.name_first_name or ""
                    lname = user_details.name_last_name or ""
                    full_name = f"{fname} {lname}".strip()
                    if full_name:
                        node_label = f"{full_name} ({target_name})"

                    node_details = {
                        "Email": user_details.email,
                        "Department": user_details.department,
                        "Site": user_details.site,
                        "Status": user_details.status_message,
                    }
                    node_details = {k: v for k, v in node_details.items() if v}
            elif target_type_hint == "auto_attendant":
                bg_color = "#FFD700"  # Gold for AA
                owner = None
                prompt = None

                if ":" in target_name:
                    owner, prompt = target_name.split(":", 1)

                if owner and prompt:
                    try:
                        aa_resp = await get_aa_response(owner, prompt)
                        if aa_resp:
                            name = aa_resp.attendant_name or "Auto Attendant"
                            start = aa_resp.starting_prompt or prompt

                            node_details = {
                                "Attendant Name": name,
                                "Starting Prompt": start,
                                "Owner": owner,
                            }

                            # Intro Greeting Detection
                            is_intro = False
                            if "Announce" in prompt:
                                found_script = None
                                found_timeframe = None
                                found_ordinal = None

                                digits = re.findall(r"\d+", prompt)
                                if digits and aa_resp.intro_greetings:
                                    target_id = int(digits[-1])
                                    for greeting in aa_resp.intro_greetings:
                                        if isinstance(greeting, dict):
                                            audio = greeting.get("audio", {})
                                            if audio.get("ordinal-order") == target_id:
                                                found_script = audio.get(
                                                    "file-script-text"
                                                )
                                                found_timeframe = greeting.get(
                                                    "time-frame"
                                                )
                                                found_ordinal = target_id
                                                break
                                if found_script:
                                    node_details["Intro Script"] = found_script

                                if found_timeframe and found_ordinal:
                                    node_label = f"Intro Greeting: {found_timeframe} ({found_ordinal})"
                                    is_intro = True
                                    # Set Parent to Main AA
                                    # Construct Main AA ID. Assuming Main AA ID format matches standard AA.
                                    # If Main AA is 'owner:start', then ID is auto_attendant_owner_start
                                    main_aa_id_raw = f"auto_attendant_{owner}_{start}"
                                    node_parent = self._safe_id(main_aa_id_raw)

                            if not is_intro:
                                node_label = f"{name} ({start})"
                        else:
                            node_label = f"Auto Attendant: {prompt}"
                    except Exception as e:
                        logger.warning(f"Error fetching AA details: {e}")
                        node_label = f"Auto Attendant: {prompt}"
                else:
                    node_label = f"Auto Attendant: {target_name}"

                # If not intro greeting, check for user grouping
                if not node_parent and source_id.startswith("user_"):
                    node_parent = self._safe_id(source_id)

                # If nested AA, make it a child of the source AA
                if (
                    not node_parent
                    and source_id.startswith("auto_attendant_")
                    and "nested_" in target_name
                ):
                    node_parent = self._safe_id(source_id)

            elif target_type_hint == "call_queue":
                bg_color = "#FFA500"  # Orange for Queue
                node_label = f"Queue: {target_name}"
                potential_parent = self._safe_id(f"user_{target_name}")
                if source_id == potential_parent:
                    node_parent = potential_parent

            elif target_type_hint == "voicemail":
                bg_color = "#A9A9A9"  # Dark Grey for Voicemail
                if "vmail_" in target_name:
                    node_label = f"Voicemail ({target_name.replace('vmail_', '')})"

            elif target_type_hint == "offnet":
                bg_color = "#90EE90"  # Light Green for External
                node_label = f"External: {format_phone_number(target_name)}"
                node_link = None

            elif target_type_hint == "hangup":
                bg_color = "#FF6347"  # Tomato for Hangup
                node_label = "Hangup"
                node_link = None

            elif target_type_hint == "other":
                bg_color = "#D3D3D3"  # Light Grey
                node_label = f"Other: {target_name}"
                node_link = None

            elif target_type_hint == "directory":
                bg_color = "#DA70D6"  # Orchid for Directory
                node_label = "Directory"
                node_link = None

            elif target_type_hint == "conference":
                bg_color = "#EE82EE"  # Violet for Conference
                node_label = f"Conference Bridge: {target_name}"
                # Link is handled by generate_portal_link ("conference")

            elif target_type_hint == "device":
                bg_color = "#D8BFD8"  # Thistle
                node_label = f"Device: {target_name}"
                node_link = None

            elements.append(
                CytoscapeElement(
                    data=NodeData(
                        id=node_id,
                        label=node_label,
                        type=target_type_hint,
                        bg=bg_color,
                        link=node_link,
                        parent=node_parent,
                        details=node_details,
                    )
                )
            )

            # Expand Children
            if should_expand:
                children = await self._expand_node(target_name, target_type_hint)
                for (
                    child_name,
                    child_type,
                    child_label,
                    child_extra,
                    child_should_expand,
                    child_parent,
                ) in children:
                    queue.append(
                        (
                            node_id,
                            child_name,
                            child_type,
                            child_label,
                            child_extra,
                            child_should_expand,
                            child_parent,
                        )
                    )

        return elements

    async def _expand_node(
        self, node_name: str, node_type: str
    ) -> List[Tuple[str, str, str, Dict[str, Any], bool, Optional[str]]]:
        # Returns (child_name, child_type, label, extra_data, should_expand, parent_hint)
        children = []

        if node_type == "user":
            if node_name in self.rules_cache:
                rules = self.rules_cache[node_name]
            else:
                rules = await self.client.get_answer_rules(self.domain, node_name)
                self.rules_cache[node_name] = rules

            for rule in rules:
                tf_label = rule.time_frame
                if tf_label == "*":
                    tf_label = "Default"

                extra = {
                    "timeframe": tf_label,
                    "priority": rule.priority,
                    "time_range_data": rule.time_range_data,
                }

                def format_edge_label(action: str) -> str:
                    return f"{action} (Timeframe: {tf_label})"

                # Simultaneous Ring
                if rule.simultaneous_ring and rule.simultaneous_ring.enabled == "yes":
                    for target in rule.simultaneous_ring.parameters:
                        if target:
                            lbl = format_edge_label("Simultaneous Ring")
                            child_type, child_name, child_parent = (
                                self._get_type_and_name(target)
                            )

                            if child_type == "auto_attendant":
                                # Scoping: If AA target doesn't have ':', assume it belongs to current node (user)
                                if ":" not in child_name:
                                    child_name = f"{node_name}:{child_name}"

                            children.append(
                                (child_name, child_type, lbl, extra, True, child_parent)
                            )

                # Forwarding
                forward_mappings = [
                    ("Forward Always", rule.forward_always),
                    ("Forward Busy", rule.forward_on_busy),
                    ("Forward No Answer", rule.forward_no_answer),
                    ("Forward Offline", rule.forward_when_unregistered),
                ]

                for fwd_name, fwd_logic in forward_mappings:
                    if (
                        fwd_logic
                        and fwd_logic.enabled == "yes"
                        and fwd_logic.parameters
                    ):
                        target = fwd_logic.parameters[0]
                        if target:
                            lbl = format_edge_label(fwd_name)
                            child_type, child_name, child_parent = (
                                self._get_type_and_name(target)
                            )

                            if child_type == "auto_attendant":
                                if ":" not in child_name:
                                    child_name = f"{node_name}:{child_name}"

                            children.append(
                                (child_name, child_type, lbl, extra, True, child_parent)
                            )

        elif node_type == "auto_attendant":
            if ":" in node_name:
                owner, prompt = node_name.split(":", 1)
            else:
                owner = node_name
                prompt = node_name

            logger.debug(f"Expanding Auto Attendant: owner={owner}, prompt={prompt}")
            try:
                # Use cache if available (populated by _process_did_path or previous calls)
                cache_key = f"{owner}:{prompt}"
                if cache_key in self.aa_prompts_cache:
                    aa_response = self.aa_prompts_cache[cache_key]
                else:
                    aa_response = await self.client.get_auto_attendant_prompts(
                        self.domain, owner, prompt
                    )
                    self.aa_prompts_cache[cache_key] = aa_response

                if aa_response:
                    # Check if this node is actually an Intro Greeting
                    is_intro = False
                    main_prompt = aa_response.starting_prompt

                    if "Announce" in prompt and aa_response.intro_greetings:
                        digits = re.findall(r"\d+", prompt)
                        if digits:
                            target_id = int(digits[-1])
                            for greeting in aa_response.intro_greetings:
                                if isinstance(greeting, dict):
                                    audio = greeting.get("audio", {})
                                    if audio.get("ordinal-order") == target_id:
                                        # Found it! It's an Intro Greeting.
                                        is_intro = True
                                        break

                    if is_intro and main_prompt:
                        # It is an intro greeting.
                        # Return ONE child: The Main AA.
                        # Logic: Intro -> Main AA.
                        # The main AA name needs to be reconstructed.
                        # Usually owner + starting_prompt
                        child_name = f"{owner}:{main_prompt}"
                        children.append(
                            (child_name, "auto_attendant", "Next", {}, True, None)
                        )

                    elif aa_response.auto_attendant:
                        # Normal AA expansion
                        for key, option in aa_response.auto_attendant.items():
                            # Determine Label
                            label = key
                            if key == "no-key-press":
                                label = "No Input"
                            elif key == "unassigned-key-press":
                                label = "Invalid Input"
                            elif key.startswith("option-"):
                                label = f"Press {key.replace('option-', '')}"
                            elif not (
                                key == "no-key-press"
                                or key == "unassigned-key-press"
                                or key.startswith("option-")
                            ):
                                # Skip unknown keys if strictly filtering, but let's allow basic iteration
                                continue

                            # Handle String Values (e.g., "repeat")
                            if isinstance(option, str):
                                if option == "repeat":
                                    # Loop back to self
                                    children.append(
                                        (
                                            node_name,
                                            "auto_attendant",
                                            label,
                                            {},
                                            False,
                                            None,
                                        )
                                    )
                                continue

                            # Handle Object Values
                            # Check for Nested AA
                            if option.nested_aa:
                                # Create synthetic ID and Object
                                synthetic_suffix = f"nested_{key}"
                                synthetic_prompt = f"{prompt}:{synthetic_suffix}"
                                synthetic_id = f"{owner}:{synthetic_prompt}"

                                try:
                                    # We need to wrap the nested dict into the expected structure
                                    nested_resp = (
                                        NSAutoAttendantResponse.model_validate(
                                            {
                                                "user": owner,
                                                "starting-prompt": synthetic_prompt,
                                                "attendant-name": f"Nested {label}",
                                                "auto-attendant": option.nested_aa,
                                            }
                                        )
                                    )
                                    # Cache it so next lookup finds it
                                    self.aa_prompts_cache[synthetic_id] = nested_resp
                                    children.append(
                                        (
                                            synthetic_id,
                                            "auto_attendant",
                                            label,
                                            {},
                                            True,
                                            None,
                                        )
                                    )
                                except Exception as e:
                                    logger.warning(
                                        f"Failed to parse nested AA for {key}: {e}"
                                    )
                                continue

                            # Check for Company Directory
                            app = option.destination_application
                            if app and "sip:start" in app and "directory" in app:
                                children.append(
                                    ("Directory", "directory", label, {}, False, None)
                                )
                                continue

                            # Standard Destinations
                            dest = option.destination_user

                            # Use _get_type_and_name on the raw destination first
                            child_type, child_name, child_parent = (
                                self._get_type_and_name(dest or "")
                            )

                            if app == "hangup":
                                child_type = "hangup"
                                child_name = "Hangup"
                            elif app == "voicemail":
                                child_type = "voicemail"
                            elif app == "repeat-tier":
                                child_type = "auto_attendant"
                                child_name = node_name
                            elif app == "callcenter":
                                child_type = "call_queue"
                            elif app == "to-single-device":
                                # Check for Conference
                                # Pattern: ID.something (e.g., 3333.1234.com)
                                if dest and "." in dest:
                                    conf_id = dest.split(".")[0]
                                    child_name = conf_id
                                    child_type = "conference"

                            if child_name:
                                children.append(
                                    (
                                        child_name,
                                        child_type,
                                        label,
                                        {},
                                        True,
                                        child_parent,
                                    )
                                )
            except Exception as e:
                logger.warning(f"Failed to fetch AA prompts for {owner}/{prompt}: {e}")

        elif node_type == "call_queue":
            logger.debug(f"Expanding Call Queue: {node_name}")
            try:
                if node_name in self.queue_agents_cache:
                    agents = self.queue_agents_cache[node_name]
                else:
                    agents = await self.client.get_call_queue_agents(
                        self.domain, node_name
                    )
                    self.queue_agents_cache[node_name] = agents

                for agent in agents:
                    if agent.user:
                        # Agents are terminal in the context of a queue
                        children.append((agent.user, "user", "Agent", {}, False, None))
            except Exception as e:
                logger.warning(f"Failed to fetch agents for queue {node_name}: {e}")

        return children
