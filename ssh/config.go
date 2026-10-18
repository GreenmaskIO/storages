// Copyright 2026 Greenmask
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ssh

import (
	"fmt"
	"time"
)

const (
	defaultPort           = 22
	defaultConnectTimeout = 30 * time.Second
)

type Config struct {
	Host                 string        // required
	Port                 int           // default 22
	User                 string        // required
	Password             string        // auth: password
	PrivateKeyPath       string        // auth: private key
	PrivateKeyPassphrase string        // decrypts an encrypted private key
	Prefix               string        // remote root path
	ConnectTimeout       time.Duration // bounds the TCP dial and the SSH handshake, default 30s
}

// DefaultConfig returns a Config populated with sensible defaults. Set the
// required fields (Host, User) and the credentials on the result before passing
// it to New.
func DefaultConfig() Config {
	c := Config{}
	c.applyDefaults()
	return c
}

// applyDefaults fills unset fields with their defaults so a Config built as a
// struct literal behaves like one from DefaultConfig. New calls this, so
// Config{Host: "...", User: "...", Password: "..."} is a complete config.
func (c *Config) applyDefaults() {
	if c.Port == 0 {
		c.Port = defaultPort
	}
	if c.ConnectTimeout == 0 {
		c.ConnectTimeout = defaultConnectTimeout
	}
}

func (c *Config) Validate() error {
	c.applyDefaults()
	if c.Host == "" {
		return fmt.Errorf("host is required")
	}
	if c.User == "" {
		return fmt.Errorf("user is required")
	}
	if c.Password == "" && c.PrivateKeyPath == "" {
		return fmt.Errorf("one of password or private_key_path is required")
	}
	if c.PrivateKeyPassphrase != "" && c.PrivateKeyPath == "" {
		return fmt.Errorf("private_key_passphrase requires private_key_path")
	}
	if c.ConnectTimeout < 0 {
		return fmt.Errorf("connect_timeout must not be negative")
	}
	return nil
}
