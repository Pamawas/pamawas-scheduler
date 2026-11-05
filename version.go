package main

import (
	"fmt"
	"os"
	"runtime/debug"
	"strings"
)

func loadVersion() string {
	if content, err := os.ReadFile("version.txt"); err == nil {
		if version := strings.TrimSpace(string(content)); version != "" {
			return version
		}
	}
	if info, ok := debug.ReadBuildInfo(); ok && info.Main.Version != "" && info.Main.Version != "(devel)" {
		return info.Main.Version
	}
	return "dev"
}

func printStartupBanner(version, port string) {
	banner := ` _____       /\      __  __      /\     __          __     /\      _____
|  __ \     /  \    |  \/  |    /  \    \ \        / /    /  \    / ____|
| |__) |   / /\ \   | \  / |   / /\ \    \ \  /\  / /    / /\ \   \___ \
|  ___/   / ____ \  | |\/| |  / ____ \    \ \/  \/ /    / ____ \   ___) |
|_|      /_/    \_\ |_|  |_| /_/    \_\    \__/\__/    /_/    \_\ |____/`
	serviceArt := ` _____    _____   _    _   ______   _____    _    _   _        ______   _____
/ ____|  / ____| | |  | | |  ____| |  __ \  | |  | | | |      |  ____| |  __ \
\___ \  | |      | |__| | | |__    | |  | | | |  | | | |      | |__    | |__) |
 ___) | | |      |  __  | |  __|   | |  | | | |__| | | |____  |  __|   |  _  /
|____/   \_____| |_|  |_| |______| |_____/   \____/  |______| |______| |_| \_\`
	fmt.Printf("\033[1;36m%s\n%s\033[0m\nVersion: %s | Port: :%s\n\n", banner, serviceArt, version, port)
}
