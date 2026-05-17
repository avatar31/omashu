/*
 * SPDX-FileCopyrightText: © 2026 Sachin S
 * SPDX-License-Identifier: Apache-2.0
 */

package utils

import (
	"encoding/binary"
	"os"
	"path/filepath"
)

func Uint64ToBytes(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func BytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

func CreateDirIfNotExists(folderPath string) error {
	if !IsDirExists(folderPath) {
		return CreateDir(folderPath)
	}
	return nil
}

func CreateDir(dirPath string) error {
	return os.MkdirAll(dirPath, 0755)
}

func IsDirExists(dirPath string) bool {
	info, err := os.Stat(dirPath)
	if err != nil {
		return false
	}
	return info.IsDir()
}

func CreateFileIfNotExists(path string) (*os.File, error) {
	if !IsFileExists(path) {
		return CreateFile(path)
	}

	return OpenFile(path)
}

func CreateFile(path string) (*os.File, error) {
	return os.OpenFile(
		path,
		os.O_CREATE|os.O_EXCL|os.O_WRONLY,
		0644,
	)
}

func OpenFile(path string) (*os.File, error) {
	return os.OpenFile(
		path,
		os.O_APPEND|os.O_WRONLY,
		0644,
	)
}

func IsFileExists(filePath string) bool {
	_, err := os.Stat(filePath)
	return err == nil
}

func RemoveMatchFile(dir string, remove func(string) bool) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		filename := entry.Name()
		if remove(filename) {
			if err := os.RemoveAll(filepath.Join(dir, filename)); err != nil {
				return err
			}
		}
	}
	return nil
}
