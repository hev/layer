package tui

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/hev/layer/apps/layer-cli/internal/config"
	"github.com/hev/layer/apps/layer-cli/internal/keys"
	hevlayer "github.com/hev/layer/clients/go"
)

const (
	refreshInterval = 2 * time.Second
	// snapshotWindow bounds the global snapshot-activity feed used to derive
	// each index's last-snapshot age. Indexes that have not snapshotted within
	// this window show "—" in the list; their true history lives in the detail
	// view. The feed is capped server-side at 500 events.
	snapshotWindow = 90 * 24 * time.Hour
	historyLimit   = 500
)

type Options struct {
	HomeDir string
	Env     map[string]string
	Request config.ResolveRequest
}

type view int

const (
	viewEnvs view = iota
	viewFunctions
	viewFunctionDetail
	viewIndexes
	viewIndexDetail
	viewPipelines
	viewPipelineDetail
	viewKeys
	viewKeyDetail
)

type Model struct {
	opts     Options
	view     view
	width    int
	height   int
	resolved config.Resolved
	client   *hevlayer.Client
	err      error
	license  *hevlayer.LicenseState

	envs           envsModel
	functions      functionsModel
	detail         detailModel
	indexes        indexesModel
	indexDetail    indexDetailModel
	pipelines      pipelinesModel
	pipelineDetail pipelineDetailModel
	keys           keysModel
	keyDetail      keyDetailModel
}

type envsModel struct {
	entries []config.EnvEntry
	cursor  int
	err     error
}

type functionItem struct {
	id         string
	paused     bool
	pending    int64
	processing int64
	failed     int64
	sweeps     int64
	rate       float64
}

type functionsModel struct {
	items   []functionItem
	cursor  int
	loading bool
	err     error
}

type detailModel struct {
	id      string
	status  *hevlayer.UdfStatus
	loading bool
	err     error
}

type indexesModel struct {
	items    []hevlayer.NamespaceListEntry
	lastSnap map[string]int64 // namespace -> last snapshot write (epoch ms); 0/absent = none in window
	cursor   int
	loading  bool
	err      error
	snapErr  error // non-fatal: indexes still render if the activity feed fails
}

type indexDetailModel struct {
	name     string
	entry    hevlayer.NamespaceListEntry
	history  []hevlayer.SnapshotHistoryEntry
	moreThan bool // history hit the fetch cap, so the real count is higher
	loading  bool
	err      error
}

// pipelineItem is a gateway pipeline joined with its live queue status; the
// queue is nil until a worker registers.
type pipelineItem struct {
	hevlayer.Pipeline
	queue *hevlayer.PipelineStatus
}

type pipelinesModel struct {
	items   []pipelineItem
	cursor  int
	loading bool
	err     error
}

type pipelineDetailModel struct {
	name     string // the selected pipeline ID
	pipeline hevlayer.Pipeline
	queue    *hevlayer.PipelineStatus // gateway queue depth; nil when no queue is registered
	loading  bool
	err      error
}

// keysModel lists gateway API keys — metadata only, never tokens or hashes.
// The view is read-only like the rest of the TUI: minting and revoking stay
// in the `layer keys` commands.
type keysModel struct {
	items   []keys.Key
	cursor  int
	loading bool
	err     error
}

type keyDetailModel struct {
	keyID   string // the selected key's keyId
	key     keys.Key
	loading bool
	err     error
}

type functionsLoadedMsg struct {
	items []functionItem
	err   error
}

type detailLoadedMsg struct {
	status *hevlayer.UdfStatus
	err    error
}

type indexesLoadedMsg struct {
	items    []hevlayer.NamespaceListEntry
	lastSnap map[string]int64
	snapErr  error
	err      error
}

type indexDetailLoadedMsg struct {
	history  []hevlayer.SnapshotHistoryEntry
	moreThan bool
	err      error
}

type pipelinesLoadedMsg struct {
	items []pipelineItem
	err   error
}

type pipelineDetailLoadedMsg struct {
	pipeline *hevlayer.Pipeline
	queue    *hevlayer.PipelineStatus
	err      error
}

type keysLoadedMsg struct {
	items []keys.Key
	err   error
}

type keyDetailLoadedMsg struct {
	key *keys.Key
	err error
}

type tickMsg struct{}

type licenseLoadedMsg struct {
	license *hevlayer.LicenseState
}

// Brand palette lifted from the operations dashboard
// (apps/layer-dashboard/static/index.html). lipgloss downsamples these hex
// values to the nearest color the terminal supports.
var (
	brandSignal     = lipgloss.Color("#e25822") // primary accent (rust/orange)
	brandSignalDark = lipgloss.Color("#ff6b33") // brighter accent — errors, emphasis
	brandOK         = lipgloss.Color("#6fae6a") // healthy / caught up
	brandMuted      = lipgloss.Color("#6b6b66") // secondary text
)

var (
	titleStyle  = lipgloss.NewStyle().Bold(true).Foreground(brandSignal)
	mutedStyle  = lipgloss.NewStyle().Foreground(brandMuted)
	errorStyle  = lipgloss.NewStyle().Foreground(brandSignalDark)
	activeStyle = lipgloss.NewStyle().Bold(true).Foreground(brandSignal)
	goodStyle   = lipgloss.NewStyle().Foreground(brandOK)
	warnStyle   = lipgloss.NewStyle().Foreground(brandSignal)

	colName   = lipgloss.NewStyle().Width(28)
	colRows   = lipgloss.NewStyle().Width(8).Align(lipgloss.Right)
	colSize   = lipgloss.NewStyle().Width(8).Align(lipgloss.Right)
	colWrite  = lipgloss.NewStyle().Width(8).Align(lipgloss.Right)
	colStable = lipgloss.NewStyle().Width(8)
	colUnidx  = lipgloss.NewStyle().Width(8).Align(lipgloss.Right)
	colSnap   = lipgloss.NewStyle().Width(7).Align(lipgloss.Right)
	colCache  = lipgloss.NewStyle().Width(6)
)

func New(opts Options) Model {
	model := Model{opts: opts}
	entries, err := config.ListEnvs(opts.HomeDir)
	model.envs = envsModel{entries: entries, err: err}
	if len(entries) > 1 && !opts.Request.EnvNameSet && strings.TrimSpace(opts.Env["LAYER_ENV"]) == "" {
		model.view = viewEnvs
		return model
	}
	model.view = viewIndexes
	model.indexes.loading = true
	return model
}

func (m Model) Init() tea.Cmd {
	// Start the single refresh tick up front and let tickMsg perpetuate it across
	// every view. View switches (navKey, drill-in, back-out) never spawn their own
	// tick, so the refresh cadence stays at exactly one timer regardless of
	// navigation.
	if m.view == viewEnvs {
		return tickCmd()
	}
	return tea.Batch(m.loadIndexes(), m.loadLicense(), tickCmd())
}

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
	case tea.KeyMsg:
		if msg.String() == "ctrl+c" {
			return m, tea.Quit
		}
	case tickMsg:
		licenseCmd := m.loadLicense()
		switch m.view {
		case viewFunctions:
			return m, tea.Batch(m.loadFunctions(), licenseCmd, tickCmd())
		case viewFunctionDetail:
			return m, tea.Batch(m.loadDetail(), licenseCmd, tickCmd())
		case viewIndexes:
			return m, tea.Batch(m.loadIndexes(), licenseCmd, tickCmd())
		case viewIndexDetail:
			return m, tea.Batch(m.loadIndexDetail(), licenseCmd, tickCmd())
		case viewPipelines:
			return m, tea.Batch(m.loadPipelines(), licenseCmd, tickCmd())
		case viewPipelineDetail:
			return m, tea.Batch(m.loadPipelineDetail(), licenseCmd, tickCmd())
		case viewKeys:
			return m, tea.Batch(m.loadKeys(), licenseCmd, tickCmd())
		case viewKeyDetail:
			return m, tea.Batch(m.loadKeyDetail(), licenseCmd, tickCmd())
		default:
			return m, tickCmd()
		}
	case functionsLoadedMsg:
		m.functions.loading = false
		m.functions.err = msg.err
		if msg.err == nil {
			m.functions.items = msg.items
			if m.functions.cursor >= len(m.functions.items) {
				m.functions.cursor = max(0, len(m.functions.items)-1)
			}
		}
	case licenseLoadedMsg:
		if msg.license != nil {
			m.license = msg.license
		}
	case detailLoadedMsg:
		m.detail.loading = false
		m.detail.err = msg.err
		if msg.err == nil {
			m.detail.status = msg.status
		}
	case indexesLoadedMsg:
		m.indexes.loading = false
		m.indexes.err = msg.err
		if msg.err == nil {
			m.indexes.items = msg.items
			m.indexes.lastSnap = msg.lastSnap
			m.indexes.snapErr = msg.snapErr
			if m.indexes.cursor >= len(m.indexes.items) {
				m.indexes.cursor = max(0, len(m.indexes.items)-1)
			}
		}
	case indexDetailLoadedMsg:
		m.indexDetail.loading = false
		m.indexDetail.err = msg.err
		if msg.err == nil {
			m.indexDetail.history = msg.history
			m.indexDetail.moreThan = msg.moreThan
		}
	case pipelinesLoadedMsg:
		m.pipelines.loading = false
		m.pipelines.err = msg.err
		if msg.err == nil {
			m.pipelines.items = msg.items
			if m.pipelines.cursor >= len(m.pipelines.items) {
				m.pipelines.cursor = max(0, len(m.pipelines.items)-1)
			}
		}
	case pipelineDetailLoadedMsg:
		m.pipelineDetail.loading = false
		m.pipelineDetail.err = msg.err
		if msg.err == nil {
			if msg.pipeline != nil {
				m.pipelineDetail.pipeline = *msg.pipeline
			}
			m.pipelineDetail.queue = msg.queue
		}
	case keysLoadedMsg:
		m.keys.loading = false
		m.keys.err = msg.err
		if msg.err == nil {
			m.keys.items = msg.items
			if m.keys.cursor >= len(m.keys.items) {
				m.keys.cursor = max(0, len(m.keys.items)-1)
			}
		}
	case keyDetailLoadedMsg:
		m.keyDetail.loading = false
		m.keyDetail.err = msg.err
		if msg.err == nil && msg.key != nil {
			m.keyDetail.key = *msg.key
		}
	}

	switch m.view {
	case viewEnvs:
		return m.updateEnvs(msg)
	case viewFunctions:
		return m.updateFunctions(msg)
	case viewFunctionDetail:
		return m.updateDetail(msg)
	case viewIndexes:
		return m.updateIndexes(msg)
	case viewIndexDetail:
		return m.updateIndexDetail(msg)
	case viewPipelines:
		return m.updatePipelines(msg)
	case viewPipelineDetail:
		return m.updatePipelineDetail(msg)
	case viewKeys:
		return m.updateKeys(msg)
	case viewKeyDetail:
		return m.updateKeyDetail(msg)
	}
	return m, nil
}

func (m Model) View() string {
	switch m.view {
	case viewEnvs:
		return m.viewEnvs()
	case viewFunctions:
		return m.viewFunctions()
	case viewFunctionDetail:
		return m.viewDetail()
	case viewIndexes:
		return m.viewIndexes()
	case viewIndexDetail:
		return m.viewIndexDetail()
	case viewPipelines:
		return m.viewPipelines()
	case viewPipelineDetail:
		return m.viewPipelineDetail()
	case viewKeys:
		return m.viewKeys()
	case viewKeyDetail:
		return m.viewKeyDetail()
	default:
		return ""
	}
}

// footer renders the persistent shortcut bar shown at the bottom of every view:
// the global view-switch keys (with the current section emphasized) followed by
// the view's own context actions. Keeping it identical across views means the
// navigation keys stay visible no matter how deep you drill in.
func (m Model) footer(context string) string {
	nav := navLine(m.view)
	status := m.licenseBanner()
	if strings.TrimSpace(context) == "" {
		return nav + status
	}
	return nav + mutedStyle.Render("   ·   "+context) + status
}

func (m Model) licenseBanner() string {
	if m.license == nil {
		return ""
	}
	state := m.license.Gateway.State
	var line string
	var style lipgloss.Style
	switch state {
	case "licensed":
		line = fmt.Sprintf("● licensed · trial · %d days left", daysLeft(m.license.Gateway.SecondsToDeadline))
		style = goodStyle
	case "grace":
		line = fmt.Sprintf("● grace · trial expired · %d days of grace left · renew: hevlayer.com", daysLeft(m.license.Gateway.GraceSecondsRemaining))
		style = warnStyle
	case "floor":
		line = "● open gateway · Pro surfaces off · start a trial: hevlayer.com/#start-trial"
		style = mutedStyle
	default:
		return ""
	}
	return "\n" + style.Render(line)
}

func daysLeft(seconds int64) int64 {
	if seconds <= 0 {
		return 0
	}
	const day = int64(24 * time.Hour / time.Second)
	return (seconds + day - 1) / day
}

// navLine renders the i/f/p/k/e view switcher, emphasizing whichever section
// the given view belongs to (detail views count as their parent section).
func navLine(current view) string {
	sections := []struct {
		key   string
		label string
		match func(view) bool
	}{
		{"i", "indexes", func(v view) bool { return v == viewIndexes || v == viewIndexDetail }},
		{"f", "functions", func(v view) bool { return v == viewFunctions || v == viewFunctionDetail }},
		{"p", "pipelines", func(v view) bool { return v == viewPipelines || v == viewPipelineDetail }},
		{"k", "keys", func(v view) bool { return v == viewKeys || v == viewKeyDetail }},
		{"e", "envs", func(v view) bool { return v == viewEnvs }},
	}
	parts := make([]string, 0, len(sections))
	for _, s := range sections {
		seg := s.key + " " + s.label
		if s.match(current) {
			parts = append(parts, activeStyle.Render(seg))
		} else {
			parts = append(parts, mutedStyle.Render(seg))
		}
	}
	return strings.Join(parts, mutedStyle.Render("  "))
}

// navKey handles the global view-switch shortcuts (i/f/p/k/e) that are
// available from every view, so you can toggle between indexes, functions,
// pipelines, keys, and envs without first backing out to the index list. It
// returns handled=false for any other key so each view's own bindings still
// run. It never issues tickCmd: the refresh tick started in Init is perpetual,
// so a view switch only needs to fire the new view's initial load.
func (m Model) navKey(key string) (Model, tea.Cmd, bool) {
	switch key {
	case "i":
		if m.view == viewIndexes {
			return m, nil, true
		}
		m.indexes = indexesModel{loading: true}
		m.view = viewIndexes
		return m, m.loadIndexes(), true
	case "f":
		if m.view == viewFunctions {
			return m, nil, true
		}
		m.functions = functionsModel{loading: true}
		m.view = viewFunctions
		return m, m.loadFunctions(), true
	case "p":
		if m.view == viewPipelines {
			return m, nil, true
		}
		m.pipelines = pipelinesModel{loading: true}
		m.view = viewPipelines
		return m, m.loadPipelines(), true
	case "k":
		if m.view == viewKeys {
			return m, nil, true
		}
		m.keys = keysModel{loading: true}
		m.view = viewKeys
		return m, m.loadKeys(), true
	case "e":
		entries, err := config.ListEnvs(m.opts.HomeDir)
		m.envs = envsModel{entries: entries, err: err}
		if len(entries) > 0 {
			m.view = viewEnvs
		}
		return m, nil, true
	}
	return m, nil, false
}

func (m Model) updateEnvs(msg tea.Msg) (tea.Model, tea.Cmd) {
	key, ok := msg.(tea.KeyMsg)
	if !ok {
		return m, nil
	}
	switch key.String() {
	case "q":
		return m, tea.Quit
	case "esc":
		m.indexes = indexesModel{loading: true}
		m.view = viewIndexes
		return m, m.loadIndexes()
	case "down":
		if m.envs.cursor < len(m.envs.entries)-1 {
			m.envs.cursor++
		}
	case "up":
		if m.envs.cursor > 0 {
			m.envs.cursor--
		}
	case "g":
		m.envs.cursor = 0
	case "G":
		m.envs.cursor = max(0, len(m.envs.entries)-1)
	case "enter":
		if m.envs.cursor >= 0 && m.envs.cursor < len(m.envs.entries) {
			name := m.envs.entries[m.envs.cursor].Name
			if err := config.SetActive(m.opts.HomeDir, name); err != nil {
				m.envs.err = err
				return m, nil
			}
			m.opts.Request.EnvName = ""
			m.opts.Request.EnvNameSet = false
			m.client = nil
			m.indexes = indexesModel{loading: true}
			m.view = viewIndexes
			return m, m.loadIndexes()
		}
	default:
		if nm, cmd, handled := m.navKey(key.String()); handled {
			return nm, cmd
		}
	}
	return m, nil
}

func (m Model) updateIndexes(msg tea.Msg) (tea.Model, tea.Cmd) {
	key, ok := msg.(tea.KeyMsg)
	if !ok {
		return m, nil
	}
	switch key.String() {
	case "q", "esc":
		return m, tea.Quit
	case "enter":
		if item := m.selectedIndex(); item != nil {
			m.indexDetail = indexDetailModel{name: item.Name, entry: *item, loading: true}
			m.view = viewIndexDetail
			return m, m.loadIndexDetail()
		}
	case "down":
		if m.indexes.cursor < len(m.indexes.items)-1 {
			m.indexes.cursor++
		}
	case "up":
		if m.indexes.cursor > 0 {
			m.indexes.cursor--
		}
	case "g":
		m.indexes.cursor = 0
	case "G":
		m.indexes.cursor = max(0, len(m.indexes.items)-1)
	default:
		if nm, cmd, handled := m.navKey(key.String()); handled {
			return nm, cmd
		}
	}
	return m, nil
}

func (m Model) updateIndexDetail(msg tea.Msg) (tea.Model, tea.Cmd) {
	key, ok := msg.(tea.KeyMsg)
	if !ok {
		return m, nil
	}
	switch key.String() {
	case "esc", "q":
		m.view = viewIndexes
	default:
		if nm, cmd, handled := m.navKey(key.String()); handled {
			return nm, cmd
		}
	}
	return m, nil
}

func (m Model) updatePipelines(msg tea.Msg) (tea.Model, tea.Cmd) {
	key, ok := msg.(tea.KeyMsg)
	if !ok {
		return m, nil
	}
	switch key.String() {
	case "q", "esc":
		m.indexes = indexesModel{loading: true}
		m.view = viewIndexes
		return m, m.loadIndexes()
	case "enter":
		if item := m.selectedPipeline(); item != nil {
			m.pipelineDetail = pipelineDetailModel{name: item.ID, pipeline: item.Pipeline, loading: true}
			m.view = viewPipelineDetail
			return m, m.loadPipelineDetail()
		}
	case "down":
		if m.pipelines.cursor < len(m.pipelines.items)-1 {
			m.pipelines.cursor++
		}
	case "up":
		if m.pipelines.cursor > 0 {
			m.pipelines.cursor--
		}
	case "g":
		m.pipelines.cursor = 0
	case "G":
		m.pipelines.cursor = max(0, len(m.pipelines.items)-1)
	default:
		if nm, cmd, handled := m.navKey(key.String()); handled {
			return nm, cmd
		}
	}
	return m, nil
}

func (m Model) updatePipelineDetail(msg tea.Msg) (tea.Model, tea.Cmd) {
	key, ok := msg.(tea.KeyMsg)
	if !ok {
		return m, nil
	}
	switch key.String() {
	case "esc", "q":
		m.view = viewPipelines
	default:
		if nm, cmd, handled := m.navKey(key.String()); handled {
			return nm, cmd
		}
	}
	return m, nil
}

func (m Model) selectedPipeline() *pipelineItem {
	if m.pipelines.cursor < 0 || m.pipelines.cursor >= len(m.pipelines.items) {
		return nil
	}
	return &m.pipelines.items[m.pipelines.cursor]
}

func (m Model) updateKeys(msg tea.Msg) (tea.Model, tea.Cmd) {
	key, ok := msg.(tea.KeyMsg)
	if !ok {
		return m, nil
	}
	switch key.String() {
	case "q", "esc":
		m.indexes = indexesModel{loading: true}
		m.view = viewIndexes
		return m, m.loadIndexes()
	case "enter":
		if item := m.selectedKey(); item != nil {
			m.keyDetail = keyDetailModel{keyID: item.KeyID, key: *item, loading: true}
			m.view = viewKeyDetail
			return m, m.loadKeyDetail()
		}
	case "down":
		if m.keys.cursor < len(m.keys.items)-1 {
			m.keys.cursor++
		}
	case "up":
		if m.keys.cursor > 0 {
			m.keys.cursor--
		}
	case "g":
		m.keys.cursor = 0
	case "G":
		m.keys.cursor = max(0, len(m.keys.items)-1)
	default:
		if nm, cmd, handled := m.navKey(key.String()); handled {
			return nm, cmd
		}
	}
	return m, nil
}

func (m Model) updateKeyDetail(msg tea.Msg) (tea.Model, tea.Cmd) {
	key, ok := msg.(tea.KeyMsg)
	if !ok {
		return m, nil
	}
	switch key.String() {
	case "esc", "q":
		m.view = viewKeys
	default:
		if nm, cmd, handled := m.navKey(key.String()); handled {
			return nm, cmd
		}
	}
	return m, nil
}

func (m Model) selectedKey() *keys.Key {
	if m.keys.cursor < 0 || m.keys.cursor >= len(m.keys.items) {
		return nil
	}
	return &m.keys.items[m.keys.cursor]
}

func (m Model) updateFunctions(msg tea.Msg) (tea.Model, tea.Cmd) {
	key, ok := msg.(tea.KeyMsg)
	if !ok {
		return m, nil
	}
	switch key.String() {
	case "q", "esc":
		m.indexes = indexesModel{loading: true}
		m.view = viewIndexes
		return m, m.loadIndexes()
	case "enter":
		if item := m.selectedFunction(); item != nil {
			m.detail = detailModel{id: item.id, loading: true}
			m.view = viewFunctionDetail
			return m, m.loadDetail()
		}
	case "down":
		if m.functions.cursor < len(m.functions.items)-1 {
			m.functions.cursor++
		}
	case "up":
		if m.functions.cursor > 0 {
			m.functions.cursor--
		}
	case "g":
		m.functions.cursor = 0
	case "G":
		m.functions.cursor = max(0, len(m.functions.items)-1)
	default:
		if nm, cmd, handled := m.navKey(key.String()); handled {
			return nm, cmd
		}
	}
	return m, nil
}

func (m Model) updateDetail(msg tea.Msg) (tea.Model, tea.Cmd) {
	key, ok := msg.(tea.KeyMsg)
	if !ok {
		return m, nil
	}
	switch key.String() {
	case "esc", "q":
		m.view = viewFunctions
		return m, nil
	default:
		if nm, cmd, handled := m.navKey(key.String()); handled {
			return nm, cmd
		}
	}
	return m, nil
}

func (m Model) selectedFunction() *functionItem {
	if m.functions.cursor < 0 || m.functions.cursor >= len(m.functions.items) {
		return nil
	}
	return &m.functions.items[m.functions.cursor]
}

func (m Model) selectedIndex() *hevlayer.NamespaceListEntry {
	if m.indexes.cursor < 0 || m.indexes.cursor >= len(m.indexes.items) {
		return nil
	}
	return &m.indexes.items[m.indexes.cursor]
}

func (m Model) clientForView() (*hevlayer.Client, error) {
	if m.client != nil {
		return m.client, nil
	}
	resolved, err := config.Resolve(m.opts.HomeDir, m.opts.Env, m.opts.Request)
	if err != nil {
		return nil, err
	}
	m.resolved = resolved
	m.client = hevlayer.NewClient(
		hevlayer.WithBaseURL(resolved.BaseURL),
		hevlayer.WithAPIKey(resolved.APIKey),
	)
	return m.client, nil
}

func (m Model) loadFunctions() tea.Cmd {
	return func() tea.Msg {
		client, err := m.clientForView()
		if err != nil {
			return functionsLoadedMsg{err: err}
		}
		list, err := client.ListUdfs(context.Background())
		if err != nil {
			return functionsLoadedMsg{err: err}
		}
		items := make([]functionItem, 0, len(list.Udfs))
		for _, udf := range list.Udfs {
			status, err := client.GetUdfStatus(context.Background(), udf.ID)
			if err != nil {
				return functionsLoadedMsg{err: fmt.Errorf("%s: %w", udf.ID, err)}
			}
			items = append(items, functionItem{
				id:         udf.ID,
				paused:     status.Paused,
				pending:    status.PendingCount,
				processing: status.ProcessingCount,
				failed:     status.FailedCount,
				sweeps:     status.Discovery.SweepsCompleted,
				rate:       status.IndexedRatePerMin,
			})
		}
		sort.Slice(items, func(i, j int) bool {
			return items[i].id < items[j].id
		})
		return functionsLoadedMsg{items: items}
	}
}

func (m Model) loadLicense() tea.Cmd {
	return func() tea.Msg {
		client, err := m.clientForView()
		if err != nil {
			return licenseLoadedMsg{}
		}
		license, err := client.GetLicense(context.Background())
		if err != nil {
			return licenseLoadedMsg{}
		}
		return licenseLoadedMsg{license: license}
	}
}

func (m Model) loadDetail() tea.Cmd {
	id := m.detail.id
	return func() tea.Msg {
		client, err := m.clientForView()
		if err != nil {
			return detailLoadedMsg{err: err}
		}
		status, err := client.GetUdfStatus(context.Background(), id)
		if err != nil {
			return detailLoadedMsg{err: err}
		}
		return detailLoadedMsg{status: status}
	}
}

func (m Model) loadIndexes() tea.Cmd {
	return func() tea.Msg {
		client, err := m.clientForView()
		if err != nil {
			return indexesLoadedMsg{err: err}
		}
		var entries []hevlayer.NamespaceListEntry
		cursor := ""
		for {
			page, err := client.ListNamespaces(context.Background(), &hevlayer.ListNamespacesParams{Cursor: cursor})
			if err != nil {
				return indexesLoadedMsg{err: err}
			}
			entries = append(entries, page.Namespaces...)
			if page.NextCursor == "" {
				break
			}
			cursor = page.NextCursor
		}
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].Name < entries[j].Name
		})

		// One global call yields last-snapshot-per-namespace, newest first,
		// instead of an N+1 history fetch per row. Failure is non-fatal: the
		// list still renders with an empty snapshot column.
		lastSnap := map[string]int64{}
		var snapErr error
		since := time.Now().Add(-snapshotWindow).UnixMilli()
		activity, aerr := client.ListSnapshotActivity(context.Background(), &hevlayer.ListSnapshotActivityParams{
			Since: since,
			Limit: historyLimit,
		})
		if aerr != nil {
			snapErr = aerr
		} else {
			for _, event := range activity.Events {
				if _, seen := lastSnap[event.Namespace]; !seen {
					lastSnap[event.Namespace] = event.TsMs
				}
			}
		}
		return indexesLoadedMsg{items: entries, lastSnap: lastSnap, snapErr: snapErr}
	}
}

func (m Model) loadIndexDetail() tea.Cmd {
	name := m.indexDetail.name
	return func() tea.Msg {
		client, err := m.clientForView()
		if err != nil {
			return indexDetailLoadedMsg{err: err}
		}
		history, err := client.ListNamespaceHistory(context.Background(), name, &hevlayer.ListNamespaceHistoryParams{Limit: historyLimit})
		if err != nil {
			return indexDetailLoadedMsg{err: err}
		}
		return indexDetailLoadedMsg{history: history, moreThan: len(history) >= historyLimit}
	}
}

func (m Model) loadPipelines() tea.Cmd {
	return func() tea.Msg {
		client, err := m.clientForView()
		if err != nil {
			return pipelinesLoadedMsg{err: err}
		}
		list, err := client.ListPipelines(context.Background())
		if err != nil {
			return pipelinesLoadedMsg{err: err}
		}
		items := make([]pipelineItem, 0, len(list.Pipelines))
		for _, p := range list.Pipelines {
			item := pipelineItem{Pipeline: p}
			// Best-effort queue depth: a pipeline with no worker registered yet
			// has no queue, so a status error is non-fatal.
			if status, serr := client.GetPipelineStatus(context.Background(), p.ID); serr == nil {
				item.queue = status
			}
			items = append(items, item)
		}
		return pipelinesLoadedMsg{items: items}
	}
}

func (m Model) loadPipelineDetail() tea.Cmd {
	id := m.pipelineDetail.name
	return func() tea.Msg {
		client, err := m.clientForView()
		if err != nil {
			return pipelineDetailLoadedMsg{err: err}
		}
		list, err := client.ListPipelines(context.Background())
		if err != nil {
			return pipelineDetailLoadedMsg{err: err}
		}
		var found *hevlayer.Pipeline
		for i := range list.Pipelines {
			if list.Pipelines[i].ID == id {
				found = &list.Pipelines[i]
				break
			}
		}
		if found == nil {
			return pipelineDetailLoadedMsg{err: fmt.Errorf("pipeline %q not found", id)}
		}
		msg := pipelineDetailLoadedMsg{pipeline: found}
		if status, serr := client.GetPipelineStatus(context.Background(), found.ID); serr == nil {
			msg.queue = status
		}
		return msg
	}
}

// keysClientForView builds the key-route client from the same resolved
// environment clientForView uses; see internal/keys for why the key routes
// carry their own wire types.
func (m Model) keysClientForView() (*keys.Client, error) {
	resolved, err := config.Resolve(m.opts.HomeDir, m.opts.Env, m.opts.Request)
	if err != nil {
		return nil, err
	}
	return keys.NewClient(resolved.BaseURL, resolved.APIKey), nil
}

func (m Model) loadKeys() tea.Cmd {
	return func() tea.Msg {
		client, err := m.keysClientForView()
		if err != nil {
			return keysLoadedMsg{err: err}
		}
		items, err := client.List(context.Background(), false)
		if err != nil {
			return keysLoadedMsg{err: err}
		}
		sort.Slice(items, func(i, j int) bool {
			return items[i].Name < items[j].Name
		})
		return keysLoadedMsg{items: items}
	}
}

func (m Model) loadKeyDetail() tea.Cmd {
	keyID := m.keyDetail.keyID
	return func() tea.Msg {
		client, err := m.keysClientForView()
		if err != nil {
			return keyDetailLoadedMsg{err: err}
		}
		// Listing with revoked keys included keeps the detail view alive when
		// the selected key gets revoked under the refresh tick.
		items, err := client.List(context.Background(), true)
		if err != nil {
			return keyDetailLoadedMsg{err: err}
		}
		for i := range items {
			if items[i].KeyID == keyID {
				return keyDetailLoadedMsg{key: &items[i]}
			}
		}
		return keyDetailLoadedMsg{err: fmt.Errorf("key %q not found", keyID)}
	}
}

func (m Model) viewEnvs() string {
	var b strings.Builder
	b.WriteString(titleStyle.Render("Environments"))
	b.WriteString("\n\n")
	if m.envs.err != nil {
		return errorStyle.Render(m.envs.err.Error())
	}
	if len(m.envs.entries) == 0 {
		b.WriteString("No environments configured.\n\n")
		b.WriteString(m.footer("q quit"))
		return b.String()
	}
	for i, entry := range visibleEnvEntries(m.envs.entries, m.envs.cursor, m.height) {
		index := visibleStart(len(m.envs.entries), m.envs.cursor, m.height) + i
		cursor := " "
		if index == m.envs.cursor {
			cursor = ">"
		}
		active := " "
		if entry.IsActive {
			active = "*"
		}
		line := fmt.Sprintf("%s %s %-20s %-38s %s", cursor, active, truncate(entry.Name, 20), truncate(entry.Config.BaseURL, 38), config.MaskKey(entry.Config.APIKey))
		if entry.IsActive {
			line = activeStyle.Render(line)
		}
		b.WriteString(line)
		b.WriteByte('\n')
	}
	b.WriteString("\n")
	b.WriteString(m.footer("↑/↓ move  enter select  q quit"))
	return b.String()
}

func (m Model) viewIndexes() string {
	var b strings.Builder
	b.WriteString(titleStyle.Render("Indexes"))
	if m.resolved.EnvName != "" {
		b.WriteString(" ")
		b.WriteString(mutedStyle.Render("env:" + m.resolved.EnvName))
	}
	b.WriteString("\n\n")
	if m.indexes.loading && len(m.indexes.items) == 0 {
		b.WriteString("Loading indexes...\n")
		return b.String()
	}
	if m.indexes.err != nil {
		b.WriteString(errorStyle.Render(m.indexes.err.Error()))
		b.WriteByte('\n')
	}
	header := "  " + mutedStyle.Render(strings.Join([]string{
		colName.Render("NAME"),
		colRows.Render("ROWS"),
		colSize.Render("SIZE"),
		colWrite.Render("WRITTEN"),
		colStable.Render("STABLE"),
		colUnidx.Render("UNIDX"),
		colSnap.Render("SNAP"),
		colCache.Render("CACHE"),
	}, " "))
	b.WriteString(header)
	b.WriteByte('\n')

	now := time.Now().UnixMilli()
	for i, item := range visibleIndexes(m.indexes.items, m.indexes.cursor, m.height) {
		index := visibleStart(len(m.indexes.items), m.indexes.cursor, m.height) + i
		cursor := "  "
		if index == m.indexes.cursor {
			cursor = "> "
		}
		b.WriteString(cursor)
		b.WriteString(strings.Join([]string{
			colName.Render(truncate(item.Name, 28)),
			colRows.Render(humanCount(item.RowCount)),
			colSize.Render(humanBytes(item.SizeBytes)),
			colWrite.Render(humanAge(now, item.LastWriteMs)),
			stableCell(now, item),
			unindexedCell(item.Index),
			colSnap.Render(humanAge(now, m.indexes.lastSnap[item.Name])),
			colCache.Render(mutedStyle.Render(cacheLabel(item.CacheState.State))),
		}, " "))
		b.WriteByte('\n')
	}
	if len(m.indexes.items) == 0 && m.indexes.err == nil {
		b.WriteString("No indexes found.\n")
	}
	if m.indexes.snapErr != nil {
		b.WriteString(mutedStyle.Render("snapshot activity unavailable; SNAP column blank"))
		b.WriteByte('\n')
	}
	b.WriteString("\n")
	b.WriteString(m.footer("↑/↓ move  enter detail  q quit"))
	return b.String()
}

// stableCell renders the consistency state: a marker plus the age of the
// stable watermark (how far behind "now" the index is caught up to). Green
// when stable, red when not.
func stableCell(now int64, item hevlayer.NamespaceListEntry) string {
	age := humanAge(now, item.StableAsOfMs)
	if item.IsStable {
		return colStable.Render(goodStyle.Render("✓ " + age))
	}
	return colStable.Render(warnStyle.Render("✗ " + age))
}

// unindexedCell renders Turbopuffer's WAL backlog: how many bytes are written
// but not yet indexed. Non-zero (status "updating") is the lag you watch when a
// namespace is unstable; "up-to-date" is caught up; empty is "no signal yet"
// (also what a pre-passthrough gateway returns).
func unindexedCell(index hevlayer.IndexState) string {
	switch index.Status {
	case "updating":
		return colUnidx.Render(warnStyle.Render(humanBytes(index.UnindexedBytes)))
	case "up-to-date":
		return colUnidx.Render(mutedStyle.Render("0"))
	default:
		return colUnidx.Render(mutedStyle.Render("—"))
	}
}

func indexStatusLabel(index hevlayer.IndexState) string {
	switch index.Status {
	case "updating":
		return fmt.Sprintf("%s  %s unindexed", warnStyle.Render("updating"), humanBytes(index.UnindexedBytes))
	case "up-to-date":
		return goodStyle.Render("up-to-date")
	default:
		return "—"
	}
}

func cacheLabel(state string) string {
	if state == "" {
		return "—"
	}
	return state
}

func (m Model) viewIndexDetail() string {
	var b strings.Builder
	b.WriteString(titleStyle.Render("Index"))
	b.WriteString(" ")
	b.WriteString(titleStyle.Render(m.indexDetail.name))
	if m.resolved.EnvName != "" {
		b.WriteString(" ")
		b.WriteString(mutedStyle.Render("env:" + m.resolved.EnvName))
	}
	b.WriteString("\n\n")

	if m.indexDetail.err != nil {
		b.WriteString(errorStyle.Render(m.indexDetail.err.Error()))
		b.WriteByte('\n')
	}

	now := time.Now().UnixMilli()
	entry := m.indexDetail.entry
	field := func(label, value string) {
		b.WriteString(fmt.Sprintf("%-14s %s\n", label, value))
	}

	field("Rows", commas(entry.RowCount))
	field("Size", humanBytes(entry.SizeBytes))
	if len(entry.SchemaSummary.Fields) > 0 || entry.SchemaSummary.VectorDim > 0 {
		schema := fmt.Sprintf("%d fields", len(entry.SchemaSummary.Fields))
		if entry.SchemaSummary.VectorDim > 0 {
			schema += fmt.Sprintf(", vector dim %d", entry.SchemaSummary.VectorDim)
		}
		field("Schema", schema)
	}
	field("Last write", withAge(now, entry.LastWriteMs))

	stable := withAge(now, entry.StableAsOfMs)
	if entry.IsStable {
		stable += "  " + goodStyle.Render("[stable]")
	} else {
		stable += "  " + warnStyle.Render("[not stable]")
	}
	field("Stable as of", stable)
	field("Stable lag", stableLag(entry))
	field("Index status", indexStatusLabel(entry.Index))
	field("Cache", cacheLabel(entry.CacheState.State))

	b.WriteByte('\n')
	if m.indexDetail.loading && m.indexDetail.history == nil {
		b.WriteString("Loading snapshots...\n")
	} else {
		count := strconv.Itoa(len(m.indexDetail.history))
		if m.indexDetail.moreThan {
			count = count + "+"
		}
		last := "—"
		if len(m.indexDetail.history) > 0 {
			last = withAge(now, m.indexDetail.history[0].WatermarkMs)
		}
		field("Snapshots", count)
		field("Last snapshot", last)

		if len(m.indexDetail.history) > 0 {
			b.WriteByte('\n')
			b.WriteString(mutedStyle.Render("Recent snapshots (watermark · sha · age)"))
			b.WriteByte('\n')
			for _, snap := range visibleHistory(m.indexDetail.history, m.height) {
				b.WriteString(fmt.Sprintf("  %s  %-9s %s\n",
					formatMs(snap.WatermarkMs),
					shortSha(snap.Sha),
					mutedStyle.Render(humanAge(now, snap.WatermarkMs)),
				))
			}
		}
	}

	b.WriteString("\n")
	b.WriteString(m.footer("esc back"))
	return b.String()
}

// stableLag reports how far the stable watermark trails the last write — the
// window of data not yet durable in a snapshot.
func stableLag(entry hevlayer.NamespaceListEntry) string {
	if entry.LastWriteMs <= 0 || entry.StableAsOfMs <= 0 {
		return "—"
	}
	delta := entry.LastWriteMs - entry.StableAsOfMs
	if delta <= 0 {
		return goodStyle.Render("caught up")
	}
	return fmt.Sprintf("%s behind last write", humanDuration(time.Duration(delta)*time.Millisecond))
}

func (m Model) viewPipelines() string {
	var b strings.Builder
	b.WriteString(titleStyle.Render("Pipelines"))
	if m.resolved.EnvName != "" {
		b.WriteString(" ")
		b.WriteString(mutedStyle.Render("env:" + m.resolved.EnvName))
	}
	b.WriteString("\n\n")
	if m.pipelines.loading && len(m.pipelines.items) == 0 {
		b.WriteString("Loading pipelines...\n")
		return b.String()
	}
	if m.pipelines.err != nil {
		b.WriteString(errorStyle.Render(m.pipelines.err.Error()))
		b.WriteByte('\n')
	}
	b.WriteString(mutedStyle.Render(fmt.Sprintf("  %-24s %-20s %-8s %-8s %-8s %-8s", "ID", "TARGET", "PEND", "RUNNING", "FAILED", "RATE")))
	b.WriteByte('\n')
	for i, item := range visiblePipelines(m.pipelines.items, m.pipelines.cursor, m.height) {
		index := visibleStart(len(m.pipelines.items), m.pipelines.cursor, m.height) + i
		cursor := "  "
		if index == m.pipelines.cursor {
			cursor = "> "
		}
		pend, run, fail, rate := "—", "—", "—", "—"
		if q := item.queue; q != nil {
			pend = strconv.FormatInt(q.PendingCount, 10)
			run = strconv.FormatInt(q.ProcessingCount, 10)
			fail = strconv.FormatInt(q.FailedCount, 10)
			rate = fmt.Sprintf("%.2f", q.IndexedRatePerMin)
		}
		b.WriteString(cursor)
		b.WriteString(strings.Join([]string{
			padCell(truncate(item.ID, 24), 24),
			padCell(truncate(orDash(item.TargetNamespace), 20), 20),
			padCell(pend, 8),
			padCell(run, 8),
			padCell(fail, 8),
			padCell(rate, 8),
		}, " "))
		b.WriteByte('\n')
	}
	if len(m.pipelines.items) == 0 && m.pipelines.err == nil {
		b.WriteString("No pipelines found.\n")
	}
	b.WriteString("\n")
	b.WriteString(m.footer("↑/↓ move  enter detail  esc back"))
	return b.String()
}

func (m Model) viewPipelineDetail() string {
	var b strings.Builder
	b.WriteString(titleStyle.Render("Pipeline"))
	b.WriteString(" ")
	b.WriteString(titleStyle.Render(m.pipelineDetail.name))
	b.WriteString("\n\n")
	if m.pipelineDetail.err != nil {
		b.WriteString(errorStyle.Render(m.pipelineDetail.err.Error()))
		b.WriteByte('\n')
	}
	if m.pipelineDetail.loading && m.pipelineDetail.pipeline.ID == "" {
		b.WriteString("Loading pipeline...\n")
		return b.String()
	}

	pipeline := m.pipelineDetail.pipeline
	field := func(label, value string) {
		b.WriteString(fmt.Sprintf("%-16s %s\n", label, value))
	}
	field("ID", pipeline.ID)
	field("Target", orDash(pipeline.TargetNamespace))
	field("Distance metric", orDash(pipeline.DistanceMetric))
	field("Created", orDash(pipeline.CreatedAt))

	b.WriteByte('\n')
	if queue := m.pipelineDetail.queue; queue != nil {
		b.WriteString(mutedStyle.Render("Gateway queue"))
		b.WriteByte('\n')
		field("Pending", strconv.FormatInt(queue.PendingCount, 10))
		field("Processing", strconv.FormatInt(queue.ProcessingCount, 10))
		field("Failed", failedCell(queue.FailedCount))
		field("Rate/min", fmt.Sprintf("%.2f", queue.IndexedRatePerMin))
	} else {
		b.WriteString(mutedStyle.Render("No gateway queue registered yet."))
		b.WriteByte('\n')
	}

	b.WriteString("\n")
	b.WriteString(m.footer("esc back"))
	return b.String()
}

func failedCell(failed int64) string {
	if failed > 0 {
		return warnStyle.Render(strconv.FormatInt(failed, 10))
	}
	return "0"
}

func (m Model) viewKeys() string {
	var b strings.Builder
	b.WriteString(titleStyle.Render("Keys"))
	if m.resolved.EnvName != "" {
		b.WriteString(" ")
		b.WriteString(mutedStyle.Render("env:" + m.resolved.EnvName))
	}
	b.WriteString("\n\n")
	if m.keys.loading && len(m.keys.items) == 0 {
		b.WriteString("Loading keys...\n")
		return b.String()
	}
	if m.keys.err != nil {
		b.WriteString(errorStyle.Render(m.keys.err.Error()))
		b.WriteByte('\n')
	}
	b.WriteString(mutedStyle.Render(fmt.Sprintf("  %-12s %-20s %-12s %-8s %-28s %-8s %-8s", "KEY_ID", "NAME", "OWNER", "PHASE", "ENTITLEMENTS", "EXPIRES", "SEEN")))
	b.WriteByte('\n')
	now := time.Now()
	for i, item := range visibleKeys(m.keys.items, m.keys.cursor, m.height) {
		index := visibleStart(len(m.keys.items), m.keys.cursor, m.height) + i
		cursor := "  "
		if index == m.keys.cursor {
			cursor = "> "
		}
		b.WriteString(cursor)
		b.WriteString(strings.Join([]string{
			padCell(truncate(item.KeyID, 12), 12),
			padCell(truncate(item.Name, 20), 20),
			padCell(truncate(orDash(item.Owner), 12), 12),
			padCell(phaseCell(item.Phase), 8),
			padCell(truncate(orDash(strings.Join(keyTargets(item), ",")), 28), 28),
			padCell(humanExpiry(now, item.ExpiresAt), 8),
			padCell(humanSince(now, item.LastSeenAt), 8),
		}, " "))
		b.WriteByte('\n')
	}
	if len(m.keys.items) == 0 && m.keys.err == nil {
		b.WriteString("No keys minted.\n")
	}
	b.WriteString("\n")
	b.WriteString(m.footer("↑/↓ move  enter detail  esc back"))
	return b.String()
}

func (m Model) viewKeyDetail() string {
	var b strings.Builder
	b.WriteString(titleStyle.Render("Key"))
	b.WriteString(" ")
	b.WriteString(titleStyle.Render(m.keyDetail.key.Name))
	b.WriteString("\n\n")
	if m.keyDetail.err != nil {
		b.WriteString(errorStyle.Render(m.keyDetail.err.Error()))
		b.WriteByte('\n')
	}
	if m.keyDetail.loading && m.keyDetail.key.KeyID == "" {
		b.WriteString("Loading key...\n")
		return b.String()
	}

	key := m.keyDetail.key
	now := time.Now()
	field := func(label, value string) {
		b.WriteString(fmt.Sprintf("%-14s %s\n", label, value))
	}
	field("Key ID", key.KeyID)
	field("Name", key.Name)
	field("Owner", orDash(key.Owner))
	field("Description", orDash(key.Description))
	field("Phase", phaseCell(key.Phase))
	field("Created", rfc3339WithAge(now, key.CreatedAt))
	field("Expires", expiresWithRemaining(now, key.ExpiresAt))
	if key.RevokedAt != "" {
		field("Revoked", rfc3339WithAge(now, key.RevokedAt))
	}
	field("Last seen", rfc3339WithAge(now, key.LastSeenAt))

	b.WriteByte('\n')
	targets := keyTargets(key)
	if len(targets) == 0 {
		b.WriteString(mutedStyle.Render("No entitlements: the key authenticates but opens no route."))
		b.WriteByte('\n')
	} else {
		b.WriteString(mutedStyle.Render("Entitlements"))
		b.WriteByte('\n')
		for _, target := range targets {
			entitlement := key.Entitlements[target]
			b.WriteString("  " + target + "\n")
			if len(entitlement.Scopes) > 0 {
				b.WriteString(fmt.Sprintf("    %-12s %s\n", "scopes", strings.Join(entitlement.Scopes, ", ")))
			}
			if len(entitlement.Namespaces) > 0 {
				b.WriteString(fmt.Sprintf("    %-12s %s\n", "namespaces", strings.Join(entitlement.Namespaces, ", ")))
			}
			if len(entitlement.Claims) > 0 {
				b.WriteString(fmt.Sprintf("    %-12s %s\n", "claims", strings.Join(entitlement.Claims, ", ")))
			}
		}
	}

	b.WriteString("\n")
	b.WriteString(m.footer("esc back"))
	return b.String()
}

// keyTargets returns a key's entitlement targets, sorted.
func keyTargets(key keys.Key) []string {
	targets := make([]string, 0, len(key.Entitlements))
	for target := range key.Entitlements {
		targets = append(targets, target)
	}
	sort.Strings(targets)
	return targets
}

func phaseCell(phase string) string {
	switch phase {
	case "Active":
		return goodStyle.Render(phase)
	case "Revoked", "Expired":
		return warnStyle.Render(phase)
	case "":
		return "—"
	default:
		return phase
	}
}

func orDash(value string) string {
	if strings.TrimSpace(value) == "" {
		return "—"
	}
	return value
}

// padCell right-pads to a display width, counting runes (lipgloss.Width) rather
// than bytes so multibyte placeholders like "—" don't skew column alignment in
// the fmt-built tables.
func padCell(value string, width int) string {
	gap := width - lipgloss.Width(value)
	if gap <= 0 {
		return value
	}
	return value + strings.Repeat(" ", gap)
}

func (m Model) viewFunctions() string {
	var b strings.Builder
	b.WriteString(titleStyle.Render("Functions"))
	if m.resolved.EnvName != "" {
		b.WriteString(" ")
		b.WriteString(mutedStyle.Render("env:" + m.resolved.EnvName))
	}
	b.WriteString("\n\n")
	if m.functions.loading && len(m.functions.items) == 0 {
		b.WriteString("Loading functions...\n")
		return b.String()
	}
	if m.functions.err != nil {
		b.WriteString(errorStyle.Render(m.functions.err.Error()))
		b.WriteByte('\n')
	}
	b.WriteString(fmt.Sprintf("  %-28s %6s %8s %8s %6s %8s\n", "UDF", "PEND", "RUNNING", "FAILED", "SWEEP", "RATE"))
	for i, item := range visibleFunctions(m.functions.items, m.functions.cursor, m.height) {
		index := visibleStart(len(m.functions.items), m.functions.cursor, m.height) + i
		cursor := " "
		if index == m.functions.cursor {
			cursor = ">"
		}
		b.WriteString(fmt.Sprintf("%s %-28s %6d %8d %8d %6d %8.2f\n", cursor, truncate(item.id, 28), item.pending, item.processing, item.failed, item.sweeps, item.rate))
	}
	if len(m.functions.items) == 0 && m.functions.err == nil {
		b.WriteString("No UDFs registered.\n")
	}
	b.WriteString("\n")
	b.WriteString(m.footer("↑/↓ move  enter detail  esc back"))
	return b.String()
}

func (m Model) viewDetail() string {
	var b strings.Builder
	b.WriteString(titleStyle.Render("Function Detail"))
	b.WriteString("\n\n")
	if m.detail.loading && m.detail.status == nil {
		b.WriteString("Loading status...\n")
		return b.String()
	}
	if m.detail.err != nil {
		b.WriteString(errorStyle.Render(m.detail.err.Error()))
		b.WriteByte('\n')
	}
	if m.detail.status != nil {
		status := m.detail.status
		lastCompleted := ""
		if status.Discovery.LastCompletedAt != nil {
			lastCompleted = *status.Discovery.LastCompletedAt
		}
		b.WriteString(fmt.Sprintf("UDF: %s\n", status.UdfID))
		b.WriteString(fmt.Sprintf("Paused: %t\n", status.Paused))
		b.WriteString(fmt.Sprintf("Pending: %d\n", status.PendingCount))
		b.WriteString(fmt.Sprintf("Processing: %d\n", status.ProcessingCount))
		b.WriteString(fmt.Sprintf("Failed: %d\n", status.FailedCount))
		b.WriteString(fmt.Sprintf("Rate/min: %.2f\n", status.IndexedRatePerMin))
		b.WriteString(fmt.Sprintf("Sweeps: %d\n", status.Discovery.SweepsCompleted))
		b.WriteString(fmt.Sprintf("Last completed: %s\n", lastCompleted))
		b.WriteString(fmt.Sprintf("Active namespaces: %s\n", strings.Join(status.ActiveNamespaces, ", ")))
	}
	b.WriteString("\n")
	b.WriteString(m.footer("esc back"))
	return b.String()
}

func tickCmd() tea.Cmd {
	return tea.Tick(refreshInterval, func(time.Time) tea.Msg {
		return tickMsg{}
	})
}

func visibleEnvEntries(entries []config.EnvEntry, cursor int, height int) []config.EnvEntry {
	start, limit := visibleBounds(len(entries), cursor, height)
	return entries[start:limit]
}

func visibleFunctions(entries []functionItem, cursor int, height int) []functionItem {
	start, limit := visibleBounds(len(entries), cursor, height)
	return entries[start:limit]
}

func visibleIndexes(entries []hevlayer.NamespaceListEntry, cursor int, height int) []hevlayer.NamespaceListEntry {
	start, limit := visibleBounds(len(entries), cursor, height)
	return entries[start:limit]
}

func visiblePipelines(entries []pipelineItem, cursor int, height int) []pipelineItem {
	start, limit := visibleBounds(len(entries), cursor, height)
	return entries[start:limit]
}

func visibleKeys(entries []keys.Key, cursor int, height int) []keys.Key {
	start, limit := visibleBounds(len(entries), cursor, height)
	return entries[start:limit]
}

// visibleHistory shows the newest snapshots that fit under the detail header,
// no cursor scrolling — the recent tail is what matters operationally.
func visibleHistory(entries []hevlayer.SnapshotHistoryEntry, height int) []hevlayer.SnapshotHistoryEntry {
	rows := height - 16
	if rows < 1 {
		rows = len(entries)
	}
	if len(entries) <= rows {
		return entries
	}
	return entries[:rows]
}

func visibleBounds(length int, cursor int, height int) (int, int) {
	if length == 0 {
		return 0, 0
	}
	maxRows := height - 6
	if maxRows < 1 {
		maxRows = length
	}
	start := 0
	if cursor >= maxRows {
		start = cursor - maxRows + 1
	}
	limit := min(length, start+maxRows)
	return start, limit
}

func visibleStart(length int, cursor int, height int) int {
	start, _ := visibleBounds(length, cursor, height)
	return start
}

func truncate(value string, width int) string {
	if width <= 0 || len(value) <= width {
		return value
	}
	if width <= 3 {
		return value[:width]
	}
	return value[:width-3] + "..."
}

// humanAge renders the gap between now and an epoch-ms timestamp as a compact
// duration. A zero/absent timestamp renders as "—".
func humanAge(nowMs, tsMs int64) string {
	if tsMs <= 0 {
		return "—"
	}
	d := time.Duration(nowMs-tsMs) * time.Millisecond
	if d < 0 {
		d = 0
	}
	return humanDuration(d)
}

func humanDuration(d time.Duration) string {
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd", int(d.Hours()/24))
	}
}

// humanSince renders how long ago an RFC3339 timestamp was; "—" when absent
// or unparseable. Key metadata carries RFC3339 strings, not epoch ms.
func humanSince(now time.Time, value string) string {
	ts, ok := parseRFC3339(value)
	if !ok {
		return "—"
	}
	d := now.Sub(ts)
	if d < 0 {
		d = 0
	}
	return humanDuration(d)
}

// humanExpiry renders time remaining until an RFC3339 expiry; "expired" once
// past, "—" when the key never expires.
func humanExpiry(now time.Time, value string) string {
	ts, ok := parseRFC3339(value)
	if !ok {
		return "—"
	}
	if !ts.After(now) {
		return warnStyle.Render("expired")
	}
	return humanDuration(ts.Sub(now))
}

// rfc3339WithAge pairs an RFC3339 timestamp with its age, e.g.
// "2026-06-10T12:00:00Z (2m ago)".
func rfc3339WithAge(now time.Time, value string) string {
	if _, ok := parseRFC3339(value); !ok {
		return "—"
	}
	return fmt.Sprintf("%s (%s ago)", value, humanSince(now, value))
}

// expiresWithRemaining pairs an RFC3339 expiry with the time remaining, or
// flags it expired.
func expiresWithRemaining(now time.Time, value string) string {
	ts, ok := parseRFC3339(value)
	if !ok {
		return "—"
	}
	if !ts.After(now) {
		return value + "  " + warnStyle.Render("[expired]")
	}
	return fmt.Sprintf("%s (in %s)", value, humanDuration(ts.Sub(now)))
}

func parseRFC3339(value string) (time.Time, bool) {
	if strings.TrimSpace(value) == "" {
		return time.Time{}, false
	}
	ts, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return time.Time{}, false
	}
	return ts, true
}

// withAge pairs an absolute timestamp with its age, e.g.
// "2026-06-07 14:40:10Z (2m ago)".
func withAge(nowMs, tsMs int64) string {
	if tsMs <= 0 {
		return "—"
	}
	return fmt.Sprintf("%s (%s ago)", formatMs(tsMs), humanAge(nowMs, tsMs))
}

func formatMs(ms int64) string {
	if ms <= 0 {
		return "—"
	}
	return time.UnixMilli(ms).UTC().Format("2006-01-02 15:04:05Z")
}

func humanBytes(n int64) string {
	if n <= 0 {
		return "—"
	}
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%dB", n)
	}
	div, exp := int64(unit), 0
	for x := n / unit; x >= unit; x /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%cB", float64(n)/float64(div), "KMGTPE"[exp])
}

func humanCount(n int64) string {
	switch {
	case n < 0:
		return "0"
	case n < 1000:
		return strconv.FormatInt(n, 10)
	case n < 1_000_000:
		return fmt.Sprintf("%.1fk", float64(n)/1000)
	default:
		return fmt.Sprintf("%.1fM", float64(n)/1_000_000)
	}
}

func commas(n int64) string {
	s := strconv.FormatInt(n, 10)
	neg := strings.HasPrefix(s, "-")
	if neg {
		s = s[1:]
	}
	var b strings.Builder
	pre := len(s) % 3
	if pre > 0 {
		b.WriteString(s[:pre])
	}
	for i := pre; i < len(s); i += 3 {
		if b.Len() > 0 {
			b.WriteByte(',')
		}
		b.WriteString(s[i : i+3])
	}
	out := b.String()
	if neg {
		return "-" + out
	}
	return out
}

func shortSha(sha string) string {
	if len(sha) <= 7 {
		return sha
	}
	return sha[:7]
}
