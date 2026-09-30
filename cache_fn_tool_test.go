package heyicache

// skill_name: development; plugin_name: development; skill_branch: main;
// skill_commit: 125eeb496930a08d171acf2dbc7f65f1aa3c4f74;
// skill_updated_at: 2026-09-30T15:53:35+08:00; skill_version_status: unknown

import (
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/yuadsl3010/heyicache/internal/testmodel"
)

func TestGeneratedCode(t *testing.T) {
	root, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	defer os.Chdir(root)
	const module = "github.com/yuadsl3010/heyicache/generatedtest"
	infos := collectSubStructPackages(reflect.TypeOf(testmodel.Record{}), make(map[string]*TypeInfo))
	for _, info := range infos {
		validateCacheType(info.T, module)
		xxx_genCacheFn(info.T, module, "generatedtest", false, info.GetPkgs())
	}
	typ := reflect.TypeOf(testmodel.OnlyPrivate{})
	xxx_genCacheFn(typ, module, "generatedtest", false, nil)
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		code, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(code), "src.unknownFields") || strings.Contains(string(code), "dst.unknownFields") || strings.Contains(string(code), "srcBytes :=") {
			t.Fatalf("%s accesses private fields or copies the whole struct", file)
		}
	}
	mod := "module " + module + "\n\ngo 1.23\n\nrequire github.com/yuadsl3010/heyicache v0.0.0\nreplace github.com/yuadsl3010/heyicache => " + root + "\n"
	if err := os.WriteFile("go.mod", []byte(mod), 0600); err != nil {
		t.Fatal(err)
	}
	suite, err := os.ReadFile(filepath.Join(root, "testdata", "generated_test.go"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile("generated_test.go", suite, 0600); err != nil {
		t.Fatal(err)
	}
	args := []string{"test", "-mod=mod", "-count=1", "-v"}
	if extra := os.Getenv("HEYICACHE_GENERATED_TEST_FLAGS"); extra != "" {
		args = append(args, strings.Fields(extra)...)
	}
	cmd := exec.Command("go", args...)
	cmd.Env = append(os.Environ(), "GOWORK=off")
	output, err := cmd.CombinedOutput()
	t.Log(string(output))
	if err != nil {
		t.Fatal(err)
	}
}

func TestProjectionSamePackage(t *testing.T) {
	typ := reflect.TypeOf(testmodel.OnlyPrivate{})
	ct := NewCodeTool(typ.Name(), typ.PkgPath(), "testmodel", false, typ.PkgPath(), nil)
	genCacheFnProject(ct, typ.Name(), typ.Name(), getFieldTools(typ))
	code := ct.content.String()
	if strings.Contains(code, "src.") {
		t.Fatalf("private fields copied: %s", code)
	}
}

func TestUnsupportedPublicStruct(t *testing.T) {
	type Bad struct{ Public struct{ Value int } }
	defer func() {
		value := recover()
		if value == nil || !strings.Contains(value.(string), "anonymous public struct") {
			t.Fatalf("expected explicit generation failure, got %v", value)
		}
	}()
	collectSubStructPackages(reflect.TypeOf(Bad{}), make(map[string]*TypeInfo))
}

func TestLiteIntegration(t *testing.T) {
	protoc, err := exec.LookPath("protoc")
	if err != nil {
		t.Skip("protoc required for optional protobuf-go-lite integration")
	}
	plugin, err := exec.LookPath("protoc-gen-go-lite")
	if err != nil {
		t.Skip("protoc-gen-go-lite v0.19.0 required for optional integration")
	}
	root, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	for _, name := range []string{"codec", "model"} {
		if err := os.Mkdir(filepath.Join(dir, name), 0700); err != nil {
			t.Fatal(err)
		}
	}
	copyFixture := func(src, dst string) {
		data, err := os.ReadFile(filepath.Join(root, "testdata", src))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, dst), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	mod := "module github.com/yuadsl3010/heyicache/litetest\n\ngo 1.23\n\nrequire (\n github.com/yuadsl3010/heyicache v0.0.0\n github.com/aperturerobotics/protobuf-go-lite v0.19.0\n)\nreplace github.com/yuadsl3010/heyicache => " + root + "\n"
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte(mod), 0600); err != nil {
		t.Fatal(err)
	}
	copyFixture("lite.proto", "model/lite.proto")
	run := func(name string, args ...string) {
		cmd := exec.Command(name, args...)
		cmd.Dir = dir
		cmd.Env = append(os.Environ(), "GOWORK=off")
		out, err := cmd.CombinedOutput()
		t.Log(string(out))
		if err != nil {
			t.Fatal(err)
		}
	}
	run(protoc, "--plugin=protoc-gen-go-lite="+plugin, "--go-lite_out=paths=source_relative,features=marshal+unmarshal+size:.", "model/lite.proto")
	copyFixture("lite_generate_test.go", "codec/generate_test.go")
	run("go", "test", "-mod=mod", "./codec", "-run", "^TestGenerate$", "-count=1")
	copyFixture("lite_cache_test.go", "codec/cache_test.go")
	args := []string{"test", "-mod=mod", "./codec", "-run", "^TestLiteCache$", "-count=1", "-v"}
	args = append(args, strings.Fields(os.Getenv("HEYICACHE_GENERATED_TEST_FLAGS"))...)
	run("go", args...)
}
