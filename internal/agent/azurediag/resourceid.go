package azurediag

// ARM resource-ID parsing. The diagnostic stream reports resource IDs
// UPPERCASED (/SUBSCRIPTIONS/.../RESOURCEGROUPS/...), so every derived
// attribute is lowercased for stable series identity; cloud.resource_id
// keeps the verbatim form.

import "strings"

// armResource is the decomposition of one ARM resource ID.
type armResource struct {
	Subscription  string // the GUID
	ResourceGroup string
	Type          string // provider/type chain, e.g. microsoft.sql/servers/databases
	Name          string // the LAST name segment (the leaf resource)
}

// parseResourceID decomposes /subscriptions/<id>[/resourceGroups/<rg>]
// [/providers/<provider>/<type>/<name>[/<subtype>/<subname>...]]. Missing or
// malformed pieces yield empty fields, never an error — a record with an odd
// ID still exports, just with fewer attributes.
func parseResourceID(id string) armResource {
	var out armResource
	segs := strings.Split(strings.Trim(id, "/"), "/")
	var types []string
	for i := 0; i < len(segs); i++ {
		switch strings.ToLower(segs[i]) {
		case "subscriptions":
			if i+1 < len(segs) {
				out.Subscription = strings.ToLower(segs[i+1])
				i++
			}
		case "resourcegroups":
			if i+1 < len(segs) {
				out.ResourceGroup = strings.ToLower(segs[i+1])
				i++
			}
		case "providers":
			if i+1 < len(segs) {
				// A nested provider restarts the type chain: the leaf
				// provider is the one that owns the resource.
				types = types[:0]
				types = append(types, strings.ToLower(segs[i+1]))
				i++
				// Alternating <type>/<name> pairs follow.
				for i+2 < len(segs) && !strings.EqualFold(segs[i+1], "providers") {
					types = append(types, strings.ToLower(segs[i+1]))
					out.Name = strings.ToLower(segs[i+2])
					i += 2
				}
			}
		}
	}
	out.Type = strings.Join(types, "/")
	return out
}
