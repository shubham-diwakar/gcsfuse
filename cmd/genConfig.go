// Copyright 2024 Google Inc. All Rights Reserved.
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

package cmd

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var cfgOutputPath string

func NewGenConfigCmd() *cobra.Command {
	var genConfigCmd = &cobra.Command{
		Use:   "genConfig",
		Short: "Generate config file from the available configuration",
		Long: `Generate the effective config file from the available configuration
         after resolving the params passed through CLI flags and config file.
         This config file can then be used as the sole input to GCSFuse using
         the --config-file CLI flag to achieve the same effect.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return viper.SafeWriteConfigAs(cfgOutputPath)
		},
	}
	genConfigCmd.Flags().StringVar(&cfgOutputPath, "output-path", "", "Path where the output config file will be generated.")
	genConfigCmd.MarkFlagRequired("output-path")
	return genConfigCmd
}

func init() {
	rootCmd.AddCommand(NewGenConfigCmd())
}
