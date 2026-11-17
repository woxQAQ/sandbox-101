package operation

func SanitizedCommand(command Command) Command {
	result := command
	result.Env = map[string]EnvVar{}
	for key, value := range command.Env {
		if value.IsSecret() {
			value.Value = ""
		}
		if value.Inject != nil {
			copy := *value.Inject
			copy.Hosts = append([]string(nil), copy.Hosts...)
			value.Inject = &copy
		}
		result.Env[key] = value
	}
	return result
}
