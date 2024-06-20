package cmd

import (
	"fmt"

	"github.com/googlecloudplatform/gcsfuse/v2/cfg"
	"github.com/googlecloudplatform/gcsfuse/v2/internal/config"
	"github.com/mitchellh/mapstructure"
)

func PopulateConfigFromLegacyFlags(flags *flagStorage, legacyConfig *config.MountConfig) (*cfg.Config, error) {
	// TODO: This method is incomplete. We need to Populate all the configs from flags.
	resolvedConfig := &cfg.Config{}

	// Use decoder to convert flagStorage to cfg.Config.
	decoderConfig := &mapstructure.DecoderConfig{
		DecodeHook: cfg.DecodeHook(),
		Result:     resolvedConfig,
		Squash:     true,
		TagName:    "yaml",
	}
	decoder, err := mapstructure.NewDecoder(decoderConfig)
	if err != nil {
		return nil, fmt.Errorf("mapstructure.NewDecoder: %v", err)
	}
	// Decode config file first.
	//err = decoder.Decode(legacyConfig)
	//if err != nil {
	//	return nil, fmt.Errorf("decoder.Decode(flags): %v", err)
	//}
	// Decode flags after config file so that they are given precedence over config file.
	err = decoder.Decode(flags)
	if err != nil {
		return nil, fmt.Errorf("decoder.Decode(flags): %v", err)
	}

	fmt.Println(flags.LogFile, resolvedConfig.Logging.FilePath)

	return resolvedConfig, nil
}
