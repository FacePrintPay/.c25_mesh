#!/usr/bin/env python3
"""C25 Agent Mesh - Inter-Agent Communication & Coordination"""

import asyncio, json, time, hashlib, os
from pathlib import Path
from typing import Optional, List, Dict, Any, Callable, Awaitable
from dataclasses import dataclass, field, asdict
from enum import Enum
import sys
sys.path.insert(0, os.path.expanduser("~/.c25_upgrade/lib"))
sys.path.insert(0, os.path.expanduser("~/.c25_db"))

from c25_db import (
    send_agent_message, get_pending_messages, mark_message_delivered, 
    mark_message_read, save_agent_run, get_agent_history
)

class MessageType(Enum):
    TASK = "task"
    RESULT = "result"
    ERROR = "error"
    HEARTBEAT = "heartbeat"
    BROADCAST = "broadcast"
    QUERY = "query"
    KNOWLEDGE = "knowledge"

@dataclass
class AgentMessage:
    """Standardized inter-agent message"""
    id: str
    from_agent: str
    to_agent: str
    message_type: MessageType
    content: str
    priority: int = 5  # 1-10, 10 = highest
    requires_ack: bool = False
    ttl_seconds: int = 300  # Time to live
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    
    def to_dict(self) -> Dict:
        return asdict(self)
    
    @classmethod
    def create(cls, from_agent: str, to_agent: str, message_type: MessageType,
              content: str, **kwargs) -> 'AgentMessage':
        msg_id = hashlib.sha256(f"{from_agent}:{to_agent}:{time.time()}".encode()).hexdigest()[:16]
        return cls(
            id=msg_id, from_agent=from_agent, to_agent=to_agent,
            message_type=message_type, content=content, **kwargs
        )

class AgentMesh:
    """Central mesh for agent communication"""
    
    def __init__(self, agent_name: str, message_handler: Optional[Callable[[AgentMessage], Awaitable[bool]]] = None):
        self.agent_name = agent_name
        self.message_handler = message_handler
        self.running = False
        self.message_queue: asyncio.Queue = asyncio.Queue()
        
    async def start(self):
        """Start the agent's message listener"""
        self.running = True
        asyncio.create_task(self._message_listener())
        asyncio.create_task(self._heartbeat_loop())
        
    async def stop(self):
        """Stop the agent's message listener"""
        self.running = False
        
    async def _message_listener(self):
        """Listen for incoming messages from database"""
        while self.running:
            try:
                # Poll for pending messages
                messages = get_pending_messages(self.agent_name, limit=10)
                for msg_data in messages:
                    # Parse message
                    msg = AgentMessage(
                        id=msg_data['id'],
                        from_agent=msg_data['from_agent'],
                        to_agent=msg_data['to_agent'],
                        message_type=MessageType(msg_data['message_type']),
                        content=msg_data['content'],
                        metadata=json.loads(msg_data['metadata']) if msg_data['metadata'] else {}
                    )
                    
                    # Mark as delivered
                    mark_message_delivered(msg.id)
                    
                    # Queue for processing
                    await self.message_queue.put(msg)
                    
                    # If handler provided, process immediately
                    if self.message_handler:
                        try:
                            await self.message_handler(msg)
                            mark_message_read(msg.id)
                        except Exception as e:
                            # Log error but don't crash agent
                            print(f"❌ Error handling message {msg.id}: {e}")
                
                # Small delay to avoid busy polling
                await asyncio.sleep(2)
                
            except Exception as e:
                print(f"⚠️ Message listener error: {e}")
                await asyncio.sleep(5)
    
    async def _heartbeat_loop(self):
        """Send periodic heartbeats to mesh"""
        while self.running:
            try:
                # Broadcast heartbeat
                heartbeat = AgentMessage.create(
                    from_agent=self.agent_name,
                    to_agent="broadcast",
                    message_type=MessageType.HEARTBEAT,
                    content=json.dumps({"status": "alive", "timestamp": time.time()})
                )
                await self.send_message(heartbeat)
                
                await asyncio.sleep(30)  # Heartbeat every 30 seconds
                
            except Exception as e:
                print(f"⚠️ Heartbeat error: {e}")
                await asyncio.sleep(60)
    
    async def send_message(self, message: AgentMessage) -> bool:
        """Send a message to another agent (or broadcast)"""
        try:
            # Save to database
            msg_id = send_agent_message(
                from_agent=message.from_agent,
                to_agent=message.to_agent,
                message_type=message.message_type.value,
                content=message.content,
                metadata=message.metadata
            )
            
            # If sending to self, also queue for immediate processing
            if message.to_agent == self.agent_name or message.to_agent == "broadcast":
                await self.message_queue.put(message)
            
            return True
            
        except Exception as e:
            print(f"❌ Failed to send message: {e}")
            return False
    
    async def send_task(self, to_agent: str, task_description: str, 
                       priority: int = 5, requires_ack: bool = False,
                       **metadata) -> str:
        """Convenience: send a task to another agent"""
        msg = AgentMessage.create(
            from_agent=self.agent_name,
            to_agent=to_agent,
            message_type=MessageType.TASK,
            content=task_description,
            priority=priority,
            requires_ack=requires_ack,
            metadata=metadata
        )
        await self.send_message(msg)
        return msg.id
    
    async def send_result(self, to_agent: str, task_id: str, 
                         result: Any, **metadata) -> str:
        """Convenience: send a result back to requesting agent"""
        msg = AgentMessage.create(
            from_agent=self.agent_name,
            to_agent=to_agent,
            message_type=MessageType.RESULT,
            content=json.dumps({"task_id": task_id, "result": result}),
            metadata=metadata
        )
        await self.send_message(msg)
        return msg.id
    
    async def broadcast(self, message_type: MessageType, content: str, 
                       priority: int = 5, **metadata) -> str:
        """Broadcast a message to all agents"""
        msg = AgentMessage.create(
            from_agent=self.agent_name,
            to_agent="broadcast",
            message_type=message_type,
            content=content,
            priority=priority,
            metadata=metadata
        )
        await self.send_message(msg)
        return msg.id
    
    async def wait_for_message(self, message_type: Optional[MessageType] = None,
                            from_agent: Optional[str] = None,
                            timeout_seconds: float = 30.0) -> Optional[AgentMessage]:
        """Wait for a specific message with optional filters"""
        start = time.time()
        while time.time() - start < timeout_seconds:
            try:
                msg = await asyncio.wait_for(self.message_queue.get(), timeout=1.0)
                
                # Apply filters
                if message_type and msg.message_type != message_type:
                    continue
                if from_agent and msg.from_agent != from_agent:
                    continue
                
                return msg
                
            except asyncio.TimeoutError:
                continue
        
        return None

# Example: Earth Agent using the mesh
class EarthAgent:
    """Example: Earth Agent - Data Pipeline Coordinator"""
    
    def __init__(self, user_id: str):
        self.user_id = user_id
        self.mesh = AgentMesh("earth", message_handler=self.handle_message)
        self.tasks = {}
        
    async def handle_message(self, msg: AgentMessage) -> bool:
        """Handle incoming messages"""
        if msg.message_type == MessageType.TASK:
            task_data = json.loads(msg.content)
            task_id = task_data.get("task_id", hashlib.sha256(msg.content.encode()).hexdigest()[:8])
            
            # Acknowledge task
            if msg.requires_ack:
                await self.mesh.send_result(
                    to_agent=msg.from_agent,
                    task_id=task_id,
                    result={"status": "received", "task_id": task_id}
                )
            
            # Process task (example: data pipeline)
            try:
                result = await self._execute_pipeline(task_data)
                await self.mesh.send_result(
                    to_agent=msg.from_agent,
                    task_id=task_id,
                    result={"status": "completed", "output": result}
                )
                # Save execution to history
                save_agent_run(
                    agent_name="earth",
                    task_type="data_pipeline",
                    prompt=msg.content,
                    response=json.dumps(result),
                    status="completed",
                    user_id=self.user_id
                )
                return True
            except Exception as e:
                await self.mesh.send_result(
                    to_agent=msg.from_agent,
                    task_id=task_id,
                    result={"status": "error", "error": str(e)}
                )
                return False
                
        elif msg.message_type == MessageType.QUERY:
            # Handle knowledge queries
            query = json.loads(msg.content).get("query", "")
            # Simple example: return agent capabilities
            response = {"capabilities": ["data_ingestion", "etl_orchestration", "schema_mapping"]}
            await self.mesh.send_result(
                to_agent=msg.from_agent,
                task_id="query_response",
                result=response
            )
            return True
            
        return False
    
    async def _execute_pipeline(self, task_data: Dict) -> Dict:
        """Example pipeline execution (replace with real logic)"""
        await asyncio.sleep(1)  # Simulate work
        return {
            "records_processed": 1250,
            "schema_validated": True,
            "output_location": f"/data/pipeline/{task_data.get('source', 'unknown')}"
        }
    
    async def start(self):
        """Start the agent"""
        await self.mesh.start()
        print(f"🌍 Earth agent started, listening for tasks...")
        
    async def stop(self):
        """Stop the agent"""
        await self.mesh.stop()
        print(f"🌍 Earth agent stopped")

# Example: Multi-Agent Coordination
async def coordinate_complex_task(requesting_agent: str, task_description: str,
                                 required_agents: List[str], user_id: str) -> Dict:
    """Coordinate a complex task across multiple agents"""
    from_agent = requesting_agent
    
    # Step 1: Broadcast task request to required agents
    task_id = hashlib.sha256(f"{task_description}:{time.time()}".encode()).hexdigest()[:16]
    
    results = {}
    for agent_name in required_agents:
        # Send task to agent
        mesh = AgentMesh(from_agent)  # Temporary mesh for sending
        await mesh.send_task(
            to_agent=agent_name,
            task_description=json.dumps({"task_id": task_id, "description": task_description}),
            priority=8,
            requires_ack=True
        )
    
    # Step 2: Wait for results (with timeout)
    timeout = 120  # 2 minutes max
    start = time.time()
    
    while time.time() - start < timeout:
        # Check database for completed tasks
        # (In production, use WebSocket or SSE for real-time)
        await asyncio.sleep(2)
        
        # For demo: simulate results
        if time.time() - start > 10:  # After 10 seconds, return mock results
            for agent_name in required_agents:
                results[agent_name] = {
                    "status": "completed",
                    "output": f"Mock result from {agent_name} for task {task_id[:8]}"
                }
            break
    
    # Step 3: Aggregate and return results
    return {
        "task_id": task_id,
        "status": "completed" if len(results) == len(required_agents) else "partial",
        "results": results,
        "completed_agents": list(results.keys()),
        "pending_agents": [a for a in required_agents if a not in results]
    }
