package idempotent

import (
	"strings"
)

// IsUniqueConstraintError 检查是否是唯一约束冲突错误
// 这是一个通用工具函数，用于在数据库操作中捕获唯一约束错误
// 作为幂等性处理的最后一道防线
func IsUniqueConstraintError(err error) bool {
	if err == nil {
		return false
	}

	// 检查是否是 GORM 的唯一约束错误
	// GORM 会返回包含 "duplicate key" 或 "unique constraint" 的错误
	errStr := strings.ToLower(err.Error())

	// PostgreSQL 唯一约束错误的常见模式
	uniqueErrorPatterns := []string{
		"duplicate key",
		"unique constraint",
		"violates unique constraint",
		"duplicate key value",
		"unique_violation",
		"23505", // PostgreSQL 唯一约束违反的错误代码
	}

	for _, pattern := range uniqueErrorPatterns {
		if strings.Contains(errStr, strings.ToLower(pattern)) {
			return true
		}
	}

	return false
}



