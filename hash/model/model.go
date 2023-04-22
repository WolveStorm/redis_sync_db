package model

import "gorm.io/gorm"

type HSModel struct {
	gorm.Model
	DynamicKey string `gorm:"uniqueIndex"` // 用于对应redis的一行记录,这里必须使用unique，才能使用conflict，并且场景如此
	Val        string // 对应redis的json val
}
