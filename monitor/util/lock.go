package util

import "sync"

var lockMap sync.Map

func lockByName(name string) {
	lock, _ := lockMap.LoadOrStore(name, new(sync.Mutex))
	lock.(*sync.Mutex).Lock()
}

func unlockByName(name string) {
	lock, exist := lockMap.Load(name)
	if !exist {
		panic("Trying to unlock a nonexistent mutex by name: " + name)
	}
	lock.(*sync.Mutex).Unlock()
}
