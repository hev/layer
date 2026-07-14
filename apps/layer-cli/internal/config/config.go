package config

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/BurntSushi/toml"
	hevlayer "github.com/hev/layer/clients/go"
)

const (
	dirName  = ".hevlayer"
	fileName = "config.toml"
)

type Config struct {
	Active string               `toml:"active"`
	Envs   map[string]EnvConfig `toml:"envs"`
}

type EnvConfig struct {
	BaseURL       string `toml:"base_url"`
	APIKey        string `toml:"api_key"`
	KubeContext   string `toml:"kube_context,omitempty"`
	KubeNamespace string `toml:"kube_namespace,omitempty"`
}

type EnvEntry struct {
	Name     string
	Config   EnvConfig
	IsActive bool
}

type ResolveRequest struct {
	EnvName    string
	EnvNameSet bool

	BaseURL    string
	BaseURLSet bool

	APIKey    string
	APIKeySet bool

	KubeContext    string
	KubeContextSet bool

	KubeNamespace    string
	KubeNamespaceSet bool
}

type Resolved struct {
	EnvName       string
	BaseURL       string
	APIKey        string
	KubeContext   string
	KubeNamespace string
	ConfigUsed    bool
}

func DefaultHomeDir() string {
	home, _ := os.UserHomeDir()
	return home
}

func ConfigPath(homeDir string) string {
	if homeDir == "" {
		homeDir = DefaultHomeDir()
	}
	return filepath.Join(homeDir, dirName, fileName)
}

func Load(homeDir string) (Config, error) {
	cfg := Config{Envs: map[string]EnvConfig{}}
	path := ConfigPath(homeDir)
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return cfg, nil
		}
		return cfg, err
	}
	if _, err := toml.DecodeFile(path, &cfg); err != nil {
		return Config{Envs: map[string]EnvConfig{}}, err
	}
	if cfg.Envs == nil {
		cfg.Envs = map[string]EnvConfig{}
	}
	return cfg, nil
}

func Save(homeDir string, cfg Config) error {
	if homeDir == "" {
		homeDir = DefaultHomeDir()
	}
	if cfg.Envs == nil {
		cfg.Envs = map[string]EnvConfig{}
	}
	dir := filepath.Join(homeDir, dirName)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return err
	}
	path := filepath.Join(dir, fileName)
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	if err := toml.NewEncoder(file).Encode(cfg); err != nil {
		return err
	}
	return os.Chmod(path, 0o600)
}

func AddEnv(homeDir string, name string, env EnvConfig) error {
	name = strings.TrimSpace(name)
	if name == "" {
		return fmt.Errorf("environment name is required")
	}
	cfg, err := Load(homeDir)
	if err != nil {
		return err
	}
	cfg.Envs[name] = env
	if cfg.Active == "" {
		cfg.Active = name
	}
	return Save(homeDir, cfg)
}

func SetActive(homeDir string, name string) error {
	cfg, err := Load(homeDir)
	if err != nil {
		return err
	}
	if _, ok := cfg.Envs[name]; !ok {
		return fmt.Errorf("environment %q not found", name)
	}
	cfg.Active = name
	return Save(homeDir, cfg)
}

func RemoveEnv(homeDir string, name string) error {
	cfg, err := Load(homeDir)
	if err != nil {
		return err
	}
	if _, ok := cfg.Envs[name]; !ok {
		return fmt.Errorf("environment %q not found", name)
	}
	delete(cfg.Envs, name)
	if cfg.Active == name {
		cfg.Active = ""
		names := make([]string, 0, len(cfg.Envs))
		for candidate := range cfg.Envs {
			names = append(names, candidate)
		}
		sort.Strings(names)
		if len(names) > 0 {
			cfg.Active = names[0]
		}
	}
	return Save(homeDir, cfg)
}

func ListEnvs(homeDir string) ([]EnvEntry, error) {
	cfg, err := Load(homeDir)
	if err != nil {
		return nil, err
	}
	entries := make([]EnvEntry, 0, len(cfg.Envs))
	for name, env := range cfg.Envs {
		entries = append(entries, EnvEntry{
			Name:     name,
			Config:   env,
			IsActive: name == cfg.Active,
		})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].IsActive != entries[j].IsActive {
			return entries[i].IsActive
		}
		return entries[i].Name < entries[j].Name
	})
	return entries, nil
}

func GetEnv(homeDir string, name string) (EnvConfig, bool, error) {
	cfg, err := Load(homeDir)
	if err != nil {
		return EnvConfig{}, false, err
	}
	env, ok := cfg.Envs[name]
	return env, ok, nil
}

func Resolve(homeDir string, environ map[string]string, req ResolveRequest) (Resolved, error) {
	resolved := Resolved{
		BaseURL: hevlayer.DefaultBaseURL,
	}

	envSelector := ""
	if req.EnvNameSet {
		envSelector = req.EnvName
	} else {
		envSelector = strings.TrimSpace(environ["LAYER_ENV"])
	}

	envBaseURL := firstNonEmpty(environ["LAYER_BASE_URL"], environ["HEVLAYER_BASE_URL"])
	envAPIKey := firstNonEmpty(environ["LAYER_API_KEY"], environ["HEVLAYER_API_KEY"])
	directGatewayEnv := envBaseURL != "" || envAPIKey != ""

	if envSelector != "" || !directGatewayEnv {
		cfg, err := Load(homeDir)
		if err != nil {
			return resolved, err
		}
		var name string
		if envSelector != "" {
			name = envSelector
		} else {
			name = cfg.Active
		}
		if name != "" {
			env, ok := cfg.Envs[name]
			if !ok {
				return resolved, fmt.Errorf("environment %q not found", name)
			}
			resolved.EnvName = name
			resolved.BaseURL = firstNonEmpty(env.BaseURL, resolved.BaseURL)
			resolved.APIKey = env.APIKey
			resolved.KubeContext = env.KubeContext
			resolved.KubeNamespace = env.KubeNamespace
			resolved.ConfigUsed = true
		}
	}

	if envBaseURL != "" {
		resolved.BaseURL = envBaseURL
	}
	if envAPIKey != "" {
		resolved.APIKey = envAPIKey
	}
	if req.BaseURLSet {
		resolved.BaseURL = req.BaseURL
	}
	if req.APIKeySet {
		resolved.APIKey = req.APIKey
	}
	if req.KubeContextSet {
		resolved.KubeContext = req.KubeContext
	}
	if req.KubeNamespaceSet {
		resolved.KubeNamespace = req.KubeNamespace
	}
	return resolved, nil
}

func MaskKey(key string) string {
	key = strings.TrimSpace(key)
	switch {
	case key == "":
		return ""
	case len(key) <= 4:
		return strings.Repeat("*", len(key))
	case len(key) <= 8:
		return key[:2] + "..."
	default:
		return key[:8] + "..."
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
