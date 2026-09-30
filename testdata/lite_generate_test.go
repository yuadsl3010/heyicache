package codec

// skill_name: development; plugin_name: development; skill_branch: main;
// skill_commit: 125eeb496930a08d171acf2dbc7f65f1aa3c4f74;
// skill_updated_at: 2026-09-30T15:53:35+08:00; skill_version_status: unknown

import (
	"github.com/yuadsl3010/heyicache"
	"github.com/yuadsl3010/heyicache/litetest/model"
	"testing"
)

func TestGenerate(t *testing.T) { heyicache.GenCacheFn(model.Record{}) }
