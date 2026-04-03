import asyncio
import pickle
import os
import json
import time
from datetime import datetime
from typing import Dict, List, Optional
from shared.utils import logger

class RecoveryManager:
  """Manages recovery for failed nodes and checkpointing"""
  
  def __init__(self, node_id: str, storage):
    self.node_id = node_id
    self.storage = storage
    self.checkpoint_dir = f"data/{node_id}/checkpoints"
    self.checkpoint_interval = 30  # seconds
    self.running = True
    
    # Recovery state
    self.is_recovering = False
    self.last_checkpoint_time = 0
    self.checkpoint_version = 0
    
    # Create checkpoint directory
    os.makedirs(self.checkpoint_dir, exist_ok=True)
    
    logger.info(f"RecoveryManager initialized for {node_id}")
  
  async def start(self):
    """Start periodic checkpointing"""
    asyncio.create_task(self._checkpoint_loop())
    logger.info(f"Checkpointing started (every {self.checkpoint_interval}s)")
  
  async def stop(self):
    """Stop recovery manager"""
    self.running = False
  
  #This is for checkpointing
  async def _checkpoint_loop(self):
    """Periodically save checkpoint"""
    while self.running:
      await asyncio.sleep(self.checkpoint_interval)
      await self.save_checkpoint()
  
  async def save_checkpoint(self):
    """Save current system state to disk"""
    try:
      self.checkpoint_version += 1
      
      # Get all blocks
      blocks = await self.storage.list_blocks()
      
      # Collect metadata for all blocks
      metadata = {}
      for block_id in blocks:
          meta = await self.storage.get_metadata(block_id)
          if meta:
              metadata[block_id] = meta
      
      checkpoint = {
          "node_id": self.node_id,
          "timestamp": time.time(),
          "version": self.checkpoint_version,
          "block_count": len(blocks),
          "blocks": blocks,
          "metadata": metadata,
          "last_checkpoint": self.last_checkpoint_time
      }
      
      # Save to file
      filename = f"checkpoint_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{self.checkpoint_version}.pkl"
      path = os.path.join(self.checkpoint_dir, filename)
      
      with open(path, 'wb') as f:
        pickle.dump(checkpoint, f)
      
      self.last_checkpoint_time = time.time()
      logger.info(f"💾 Checkpoint saved: {len(blocks)} blocks (version {self.checkpoint_version})")
      
      # Cleanup old checkpoints
      self._cleanup_old_checkpoints()
        
    except Exception as e:
      logger.error(f"Failed to save checkpoint: {e}")
  
  def _cleanup_old_checkpoints(self, keep: int = 3):
    """Keep only the last N checkpoints"""
    try:
      files = []
      for f in os.listdir(self.checkpoint_dir):
        if f.startswith("checkpoint_") and f.endswith(".pkl"):
          path = os.path.join(self.checkpoint_dir, f)
          files.append((os.path.getmtime(path), path))
      
      files.sort(reverse=True)  # Newest first
      
      # Remove old checkpoints
      for i, (_, path) in enumerate(files):
        if i >= keep:
          os.remove(path)
          logger.info(f"Removed old checkpoint: {os.path.basename(path)}")
                
    except Exception as e:
      logger.error(f"Failed to cleanup checkpoints: {e}")
  
  async def load_latest_checkpoint(self) -> Optional[Dict]:
    """Load the most recent checkpoint"""
    try:
      # Find all checkpoint files
      files = []
      for f in os.listdir(self.checkpoint_dir):
        if f.startswith("checkpoint_") and f.endswith(".pkl"):
          path = os.path.join(self.checkpoint_dir, f)
          files.append((os.path.getmtime(path), path))
      
      if not files:
        logger.info("No checkpoints found")
        return None
      
      # Get latest
      files.sort(reverse=True)
      latest_path = files[0][1]
      
      with open(latest_path, 'rb') as f:
        checkpoint = pickle.load(f)
      
      logger.info(f"📂 Loaded checkpoint: {os.path.basename(latest_path)} ({checkpoint['block_count']} blocks)")
      return checkpoint
        
    except Exception as e:
      logger.error(f"Failed to load checkpoint: {e}")
      return None
  
  #recovery plan 
  async def plan_recovery(self, failed_node_id: str, file_map: Dict[str, List[str]]) -> Dict[str, List[str]]:
    """
    Plan recovery for a failed node
    Returns: {file_id: [source_nodes]} mapping for what needs to be replicated
    """
    logger.info(f"Planning recovery for failed node: {failed_node_id}")
    
    recovery_plan = {}
    
    # Find all files that should be on the failed node
    for file_id, nodes in file_map.items():
      # Skip internal metadata keys like _total_blocks, _all_nodes
      if file_id.startswith("_") or not isinstance(nodes, list):
          continue
          
      if failed_node_id in nodes:
          # This file needs to be restored
          # Find other nodes that have this file
          sources = [n for n in nodes if n != failed_node_id]
          if sources:
            recovery_plan[file_id] = sources

    
    logger.info(f"Recovery plan: {len(recovery_plan)} files need restoration")
    return recovery_plan
  
  async def execute_recovery(self, recovering_node: str, recovery_plan: Dict[str, List[str]]):
      """
      Execute recovery by copying missing data to recovering node
      """
      self.is_recovering = True
      logger.info(f"Executing recovery for node {recovering_node}")
      
      try:
          from shared.config import config
          import httpx
          
          recovered = 0
          failed = 0
          
          async with httpx.AsyncClient() as client:
              for file_id, sources in recovery_plan.items():
                  # Get file manifest from a source node
                  source_url = config.ALL_NODES.get(sources[0])
                  if not source_url:
                      continue
                  
                  try:
                      # Download file from source
                      response = await client.get(f"{source_url}/files/{file_id}")
                      if response.status_code == 200:
                          # Get the file content
                          content = response.content
                          
                          # Send to recovering node
                          recover_url = config.ALL_NODES.get(recovering_node)
                          if recover_url:
                              # Upload to recovering node
                              files = {'file': (file_id, content)}
                              upload_response = await client.post(
                                  f"{recover_url}/files/{file_id}",
                                  files=files
                              )
                              if upload_response.status_code == 200:
                                  recovered += 1
                              else:
                                  failed += 1
                      else:
                          failed += 1
                          
                  except Exception as e:
                      logger.error(f"Failed to recover {file_id}: {e}")
                      failed += 1
          
          logger.info(f"Recovery complete: {recovered} files restored, {failed} failed")
          
      except Exception as e:
          logger.error(f"Error during recovery execution: {e}")
      
      finally:
          self.is_recovering = False
  
  #this is teh degraded functioning mode
  async def system_status(self, live_nodes: List[str], required_quorum: int = 2) -> str:
      """
      Determine system status based on live nodes
      Returns: "HEALTHY", "DEGRADED", or "FAILED"
      """
      total_nodes = 3  # From config
      live_count = len(live_nodes)
      
      if live_count == total_nodes:
          return "HEALTHY"
      elif live_count >= required_quorum:
          return "DEGRADED"
      else:
          return "FAILED"
  
  async def handle_node_failure(self, failed_node: str, file_map: Dict[str, List[str]]):
      """
      Handle node failure - plan recovery and mark system degraded
      """
      logger.warning(f"Node {failed_node} has FAILED")
      
      # Determine system status
      live_nodes = [n for n in file_map.get("_all_nodes", []) if n != failed_node]
      status = await self.system_status(live_nodes)
      
      if status == "DEGRADED":
          logger.warning(f"System is DEGRADED (quorum maintained)")
      elif status == "FAILED":
          logger.error(f"System FAILED - cannot maintain quorum")
      
      # Plan recovery for when node returns
      recovery_plan = await self.plan_recovery(failed_node, file_map)
      
      return {
          "failed_node": failed_node,
          "system_status": status,
          "recovery_plan": recovery_plan,
          "files_to_recover": len(recovery_plan)
      }
  
  async def handle_node_recovery(self, recovered_node: str, file_map: Dict[str, List[str]]):
      """
      Handle node coming back online
      """
      logger.info(f"Node {recovered_node} is RECOVERING")
      
      # Plan what needs to be restored
      recovery_plan = await self.plan_recovery(recovered_node, file_map)
      
      if recovery_plan:
          await self.execute_recovery(recovered_node, recovery_plan)
          logger.info(f"Node {recovered_node} recovery complete")
      else:
          logger.info(f"Node {recovered_node} already in sync")
      
      return {
          "recovered_node": recovered_node,
          "files_restored": len(recovery_plan)
      }
  
  def get_checkpoint_info(self) -> Dict:
      """Get information about checkpoints"""
      files = []
      for f in os.listdir(self.checkpoint_dir):
          if f.startswith("checkpoint_") and f.endswith(".pkl"):
              path = os.path.join(self.checkpoint_dir, f)
              files.append({
                  "name": f,
                  "size": os.path.getsize(path),
                  "modified": os.path.getmtime(path)
              })
      
      files.sort(key=lambda x: x["modified"], reverse=True)
      
      return {
          "checkpoint_dir": self.checkpoint_dir,
          "total_checkpoints": len(files),
          "latest_checkpoint": files[0] if files else None,
          "checkpoints": files[:5]
      }