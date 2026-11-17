package machine

func SlotDestinationHolder(destination *QuotaDestination) string {
	switch destination.Kind {
	case "sleep":
		return "sleep:" + destination.ID
	case "claim":
		return "claim:" + destination.OperationID
	default:
		return ""
	}
}
