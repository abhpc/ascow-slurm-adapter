package utils

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

var (
	partCacheDB   *sql.DB
	partCacheMu   sync.RWMutex
	partCacheMap  map[string]string
	partCacheOnce sync.Once // 保证 DB 初始化只执行一次，防止并发重复建连
)

// cacheDBPath 返回 SQLite 文件的绝对路径：与可执行文件同目录下的 config/ 子目录。
// 用 os.Executable() 而非相对路径，保证 systemd 等方式启动时路径仍然正确。
func cacheDBPath() (string, error) {
	exe, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("cacheDBPath: %w", err)
	}
	// filepath.EvalSymlinks 处理软链接（如 go install 产生的链接）
	exe, err = filepath.EvalSymlinks(exe)
	if err != nil {
		return "", fmt.Errorf("cacheDBPath: eval symlinks: %w", err)
	}
	return filepath.Join(filepath.Dir(exe), "config", "partition_cache.db"), nil
}

// openCacheDB 打开 SQLite 文件并建表，由 partCacheOnce 保证只执行一次。
// BlockAccount/UnblockAccount 在 InitPartitionCache 未被调用时也能自动触发初始化。
func openCacheDB() error {
	var initErr error
	partCacheOnce.Do(func() {
		dbPath, err := cacheDBPath()
		if err != nil {
			initErr = err
			return
		}
		db, err := sql.Open("sqlite", dbPath)
		if err != nil {
			initErr = fmt.Errorf("openCacheDB: open sqlite: %w", err)
			return
		}
		_, err = db.Exec(`
			CREATE TABLE IF NOT EXISTS partition_allow_accounts (
				partition_name  TEXT    PRIMARY KEY,
				allow_accounts  TEXT    NOT NULL,
				updated_at      INTEGER NOT NULL
			)
		`)
		if err != nil {
			initErr = fmt.Errorf("openCacheDB: create table: %w", err)
			return
		}
		partCacheDB = db
	})
	return initErr
}

// InitPartitionCache 在程序启动时调用（logger 初始化之后）：
// 打开 SQLite、加载上次持久化的缓存、立即刷新一次，并启动后台每分钟自动刷新的 goroutine。
func InitPartitionCache() error {
	if err := openCacheDB(); err != nil {
		return err
	}

	// 进程重启时优先从磁盘加载上次缓存，避免第一次调用触发解析 slurm.conf
	_ = loadCacheFromDB()

	// 立即刷新一次，确保内存缓存为最新状态
	if err := doRefreshPartitionCache(); err != nil {
		return err
	}

	// 每分钟在后台刷新一次
	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			_ = doRefreshPartitionCache()
		}
	}()

	return nil
}

// loadCacheFromDB 从 SQLite 读取已持久化的数据并填充内存缓存。
// 仅读取非空行；若表为空则 partCacheMap 保持 nil，后续会触发全量刷新。
func loadCacheFromDB() error {
	rows, err := partCacheDB.Query(`SELECT partition_name, allow_accounts FROM partition_allow_accounts`)
	if err != nil {
		return err
	}
	defer rows.Close()

	m := make(map[string]string)
	for rows.Next() {
		var partition, accounts string
		if err := rows.Scan(&partition, &accounts); err != nil {
			return err
		}
		m[partition] = accounts
	}

	// 只有表中确实有数据时才更新内存缓存，防止用空表覆盖有效数据
	if len(m) > 0 {
		partCacheMu.Lock()
		partCacheMap = m
		partCacheMu.Unlock()
	}
	return nil
}

// doRefreshPartitionCache 解析 slurm.conf 获取最新的分区 AllowAccounts，
// 用事务全量替换 SQLite 缓存，并同步更新内存缓存。
func doRefreshPartitionCache() error {
	fresh, err := parsePartitionAllowAccounts()
	if err != nil {
		return fmt.Errorf("doRefreshPartitionCache: parse conf: %w", err)
	}

	now := time.Now().Unix()

	// 全量替换，保证 SQLite 与 slurm.conf 完全一致
	tx, err := partCacheDB.Begin()
	if err != nil {
		return fmt.Errorf("doRefreshPartitionCache: begin tx: %w", err)
	}
	if _, err := tx.Exec(`DELETE FROM partition_allow_accounts`); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("doRefreshPartitionCache: delete: %w", err)
	}
	for partition, accounts := range fresh {
		if _, err := tx.Exec(
			`INSERT INTO partition_allow_accounts(partition_name, allow_accounts, updated_at) VALUES (?,?,?)`,
			partition, accounts, now,
		); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("doRefreshPartitionCache: insert %s: %w", partition, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("doRefreshPartitionCache: commit: %w", err)
	}

	partCacheMu.Lock()
	partCacheMap = fresh
	partCacheMu.Unlock()

	return nil
}

// GetPartitionAllowAccountsFromConf 返回各分区的 AllowAccounts 映射。
//
// 数据来源优先级：
//  1. 内存缓存（最快，由 InitPartitionCache 后台维护）
//  2. SQLite 文件（进程刚重启时内存缓存为空，从磁盘恢复）
//  3. 立即生成：若 SQLite 也为空（首次运行），解析 slurm.conf 写入 SQLite 再返回
//
// 任何情况下都不会直接返回 slurm.conf 的解析结果，SQLite 始终是对外的唯一数据源。
func GetPartitionAllowAccountsFromConf() (map[string]string, error) {
	// 第一优先：内存缓存
	partCacheMu.RLock()
	cached := partCacheMap
	partCacheMu.RUnlock()
	if cached != nil {
		cp := make(map[string]string, len(cached))
		for k, v := range cached {
			cp[k] = v
		}
		return cp, nil
	}

	// 确保 SQLite 已打开（处理 InitPartitionCache 未被调用的情况）
	if err := openCacheDB(); err != nil {
		return nil, err
	}

	// 第二优先：从 SQLite 恢复（进程重启后内存缓存为空但 SQLite 有数据）
	_ = loadCacheFromDB()
	partCacheMu.RLock()
	cached = partCacheMap
	partCacheMu.RUnlock()
	if cached != nil {
		cp := make(map[string]string, len(cached))
		for k, v := range cached {
			cp[k] = v
		}
		return cp, nil
	}

	// 第三优先：SQLite 为空（首次运行），立即解析 slurm.conf 并持久化到 SQLite
	if err := doRefreshPartitionCache(); err != nil {
		return nil, err
	}
	partCacheMu.RLock()
	cached = partCacheMap
	partCacheMu.RUnlock()
	cp := make(map[string]string, len(cached))
	for k, v := range cached {
		cp[k] = v
	}
	return cp, nil
}
