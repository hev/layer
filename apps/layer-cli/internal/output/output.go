package output

import (
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
)

const (
	Table = "table"
	JSON  = "json"
	Names = "names"
)

func Validate(format string) error {
	switch format {
	case Table, JSON, Names:
		return nil
	default:
		return fmt.Errorf("-o must be one of: table, json, names")
	}
}

func WriteJSON(out io.Writer, value interface{}) error {
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(out, "%s\n", encoded)
	return err
}

func WriteTable(out io.Writer, headers []string, rows [][]string) error {
	widths := make([]int, len(headers))
	for i, header := range headers {
		widths[i] = len(header)
	}
	for _, row := range rows {
		for i, cell := range row {
			if i < len(widths) && len(cell) > widths[i] {
				widths[i] = len(cell)
			}
		}
	}
	if err := writeTableRow(out, headers, widths); err != nil {
		return err
	}
	separators := make([]string, len(headers))
	for i, width := range widths {
		separators[i] = strings.Repeat("-", width)
	}
	if err := writeTableRow(out, separators, widths); err != nil {
		return err
	}
	for _, row := range rows {
		if err := writeTableRow(out, row, widths); err != nil {
			return err
		}
	}
	return nil
}

// WriteFields renders key/value rows as left-aligned label columns, the
// non-interactive twin of the TUI detail views. Each row is a {label, value}
// pair.
func WriteFields(out io.Writer, rows [][]string) error {
	width := 0
	for _, row := range rows {
		if len(row) > 0 && len(row[0]) > width {
			width = len(row[0])
		}
	}
	for _, row := range rows {
		label, value := "", ""
		if len(row) > 0 {
			label = row[0]
		}
		if len(row) > 1 {
			value = row[1]
		}
		if _, err := fmt.Fprintf(out, "%-*s  %s\n", width, label, value); err != nil {
			return err
		}
	}
	return nil
}

func FormatInt(value int64) string {
	return strconv.FormatInt(value, 10)
}

func FormatFloat(value float64) string {
	return fmt.Sprintf("%.2f", value)
}

func writeTableRow(out io.Writer, row []string, widths []int) error {
	for i, width := range widths {
		cell := ""
		if i < len(row) {
			cell = row[i]
		}
		if i > 0 {
			if _, err := fmt.Fprint(out, "  "); err != nil {
				return err
			}
		}
		if _, err := fmt.Fprintf(out, "%-*s", width, cell); err != nil {
			return err
		}
	}
	_, err := fmt.Fprintln(out)
	return err
}
