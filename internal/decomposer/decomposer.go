// Package decomposer turns a high-level instruction into ordered subtasks.
// When GEMINI_API_KEY is set, decomposition uses Gemini (see llm.go);
// otherwise the heuristic manual splitter is used. LLM failures fall back silently.
package decomposer

import (
	"fmt"
	"os"
	"strings"

	"github.com/autonomous-ate/engine/internal/models"
)

const maxSubtasks = 10

// Decompose splits instruction into ordered subtasks for the dispatcher.
// Prefers Gemini when GEMINI_API_KEY is set; never fails fatally on LLM errors.
func Decompose(jobID, instruction, priority string) []models.SubtaskMessage {
	instruction = strings.TrimSpace(instruction)
	if instruction == "" {
		return nil
	}
	if key := strings.TrimSpace(os.Getenv("GEMINI_API_KEY")); key != "" {
		msgs, err := tryLLMDecompose(jobID, instruction, priority, key)
		if err != nil || len(msgs) == 0 {
			return decomposeManual(jobID, instruction, priority)
		}
		return msgs
	}
	return decomposeManual(jobID, instruction, priority)
}

// decomposeManual is the original deterministic splitter (also used as LLM fallback).
func decomposeManual(jobID, instruction, priority string) []models.SubtaskMessage {
	parts := splitInstruction(instruction)
	if len(parts) == 0 {
		parts = []string{strings.TrimSpace(instruction)}
	}
	if len(parts) > maxSubtasks {
		parts = mergeParts(parts, maxSubtasks)
	}
	out := make([]models.SubtaskMessage, 0, len(parts))
	for i, p := range parts {
		out = append(out, models.SubtaskMessage{
			JobID:       jobID,
			SubtaskID:   fmt.Sprintf("%s-st-%02d", jobID, i+1),
			Instruction: strings.TrimSpace(p),
			Order:       i + 1,
			Priority:    priority,
			Attempt:     1,
		})
	}
	return out
}

func splitInstruction(s string) []string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	// Prefer explicit ";" or newline boundaries, then " and ", then sentences.
	for _, sep := range []string{";", "\n"} {
		if strings.Contains(s, sep) {
			chunks := strings.Split(s, sep)
			return nonEmptyChunks(chunks)
		}
	}
	if strings.Contains(strings.ToLower(s), " and ") {
		chunks := strings.Split(s, " and ")
		return nonEmptyChunks(chunks)
	}
	// Sentence split on ". " keeping reasonable chunk sizes.
	if strings.Contains(s, ". ") {
		chunks := strings.Split(s, ". ")
		return nonEmptyChunks(chunks)
	}
	return []string{s}
}

func nonEmptyChunks(chunks []string) []string {
	out := make([]string, 0, len(chunks))
	for _, c := range chunks {
		c = strings.TrimSpace(c)
		if c != "" {
			out = append(out, c)
		}
	}
	return out
}

func mergeParts(parts []string, target int) []string {
	if len(parts) <= target {
		return parts
	}
	batch := len(parts) / target
	if batch < 1 {
		batch = 1
	}
	merged := make([]string, 0, target)
	var b strings.Builder
	count := 0
	for _, p := range parts {
		if b.Len() > 0 {
			b.WriteString("; ")
		}
		b.WriteString(p)
		count++
		if count >= batch && len(merged) < target-1 {
			merged = append(merged, b.String())
			b.Reset()
			count = 0
		}
	}
	if b.Len() > 0 {
		merged = append(merged, b.String())
	}
	return merged
}
