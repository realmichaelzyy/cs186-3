package simpledb;

import java.util.*;

public class LockManager {
	private HashMap <PageId, HashSet<TransactionId>> sharedLocks;
	private HashMap <PageId, TransactionId> exclusiveLocks;
	private HashMap <TransactionId, HashSet<PageId>> lockedSharedPages;
	private HashMap <TransactionId, HashSet<PageId>> lockedExclusivePages;
	
	public LockManager() {
		this.sharedLocks = new HashMap<PageId, HashSet<TransactionId>>();
		this.exclusiveLocks = new HashMap<PageId, TransactionId>();
		lockedSharedPages = new HashMap<TransactionId, HashSet<PageId>>();
        lockedExclusivePages = new HashMap<TransactionId, HashSet<PageId>>();
	}
	
	public void acquireLock(PageId pid, TransactionId tid, Permissions perm) throws TransactionAbortedException {
		long startTime = System.currentTimeMillis();
		long currentTime;
		boolean blocked = getLock(pid, tid, perm);
		while (!blocked) {
			blocked = getLock(pid, tid, perm);
			currentTime = System.currentTimeMillis();
			if (currentTime - startTime >= 750){
				System.out.println("Deadlock detected.");
				throw new TransactionAbortedException();
			}
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public synchronized boolean getLock(PageId pid, TransactionId tid, Permissions perm) {
		if (perm.equals(Permissions.READ_ONLY)) {
			return getSharedLock(pid, tid);
		}
		else {
			return getExclusiveLock(pid, tid);
		}
	}
	
	private synchronized boolean getSharedLock(PageId pid, TransactionId tid) {
		// if no exclusive locks for pid, give lock to transaction, else busy wait
		if (this.exclusiveLocks.get(pid) == null || this.exclusiveLocks.get(pid).equals(tid)) { 
			if (this.sharedLocks.get(pid) == null) {
				HashSet<TransactionId> tidSet = new HashSet<TransactionId>();
				tidSet.add(tid);
				this.sharedLocks.put(pid, tidSet);
			}
			else {
				this.sharedLocks.get(pid).add(tid);
			}
			
			if (this.lockedSharedPages.get(tid) == null) {
				this.lockedSharedPages.put(tid, new HashSet<PageId>());
			}
			this.lockedSharedPages.get(tid).add(pid);
			
			return true;
		}
		return false;
	}
	
	private synchronized boolean getExclusiveLock(PageId pid, TransactionId tid) {
		if (this.exclusiveLocks.get(pid) == null || this.exclusiveLocks.get(pid).equals(tid)) {
			HashSet<TransactionId> conflictingSLocks = this.sharedLocks.get(pid);
			if (conflictingSLocks != null) {
				if (conflictingSLocks.size() == 0 || (conflictingSLocks.size() == 1 && conflictingSLocks.contains(tid))){
					// upgrade lock
					this.exclusiveLocks.put(pid, tid);
					this.sharedLocks.remove(pid);
					System.out.println("If not null there is a problem in LockManager#getExclusiveLock: " + this.sharedLocks.get(pid));
					
					if(this.lockedExclusivePages.get(tid) == null) {
						this.lockedExclusivePages.put(tid, new HashSet<PageId>());
					}
					if(!this.lockedExclusivePages.get(tid).contains(pid)) {
						this.lockedExclusivePages.get(tid).add(pid);
					}
					if (this.lockedSharedPages.get(tid) != null && this.lockedSharedPages.get(tid).contains(pid)) {
						this.lockedSharedPages.get(tid).remove(pid);
					}
					return true;
				}
				return false;
			}
			this.exclusiveLocks.put(pid, tid);
			if(this.lockedExclusivePages.get(tid) == null) {
				this.lockedExclusivePages.put(tid, new HashSet<PageId>());
			}
			if(!this.lockedExclusivePages.get(tid).contains(pid)) {
				this.lockedExclusivePages.get(tid).add(pid);
			}
			return true;
		}
		return false;
	}
	
	public synchronized void releaseLock(PageId pid, TransactionId tid) {
		if (this.sharedLocks.get(pid) != null) {
			this.sharedLocks.get(pid).remove(tid);
			this.lockedSharedPages.get(tid).remove(pid);
		}
		if (tid != null && this.exclusiveLocks.get(pid) != null && this.exclusiveLocks.get(pid).equals(tid)) {
			this.exclusiveLocks.remove(pid);
			this.lockedExclusivePages.get(tid).remove(pid);
		}
	}
	
	public synchronized void releaseAllLocks(TransactionId tid) {
		// release all exclusive locks
        Set<PageId> exclusivePageIdSet = exclusiveLocks.keySet();
        Set<PageId> exclusivePageIdSetCopy = new HashSet<PageId>();
        for(PageId exclusivePageId : exclusivePageIdSet) {
                exclusivePageIdSetCopy.add(exclusivePageId);
        }
        for(PageId pageIdToRemove : exclusivePageIdSetCopy) {
                TransactionId transactionToRemove = exclusiveLocks.get(pageIdToRemove);
                if(transactionToRemove != null && transactionToRemove.equals(tid)){
                        exclusiveLocks.remove(pageIdToRemove);
                }
        }
        this.lockedExclusivePages.remove(tid);
        
        
        // release all shared locks
        Set<PageId> sharedPageIdSet = sharedLocks.keySet();
        for(PageId sharedPageId : sharedPageIdSet){
                HashSet<TransactionId> transactionIdSet = sharedLocks.get(sharedPageId);
                if(transactionIdSet != null){
                        transactionIdSet.remove(tid);
                        sharedLocks.put(sharedPageId, transactionIdSet);
                }
        }
        this.lockedSharedPages.remove(tid);
	}
	
	public synchronized boolean holdsLock(PageId pid, TransactionId tid) {
		if (this.sharedLocks.get(pid) != null) {
			if (this.sharedLocks.get(pid).contains(tid)) {
				return true;
			}
		}
		return (tid != null && this.exclusiveLocks.get(pid).equals(tid));
	}
}
