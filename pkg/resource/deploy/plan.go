// Copyright 2016-2017, Pulumi Corporation.  All rights reserved.

package deploy

import (
	"github.com/pulumi/lumi/pkg/diag"
	"github.com/pulumi/lumi/pkg/resource"
	"github.com/pulumi/lumi/pkg/resource/plugin"
	"github.com/pulumi/lumi/pkg/tokens"
	"github.com/pulumi/lumi/pkg/util/contract"
)

// TODO[pulumi/lumi#106]: plan parallelism.

// Plan is the output of analyzing resource graphs and contains the steps necessary to perform an infrastructure
// deployment.  A plan can be generated out of whole cloth from a resource graph -- in the case of new deployments --
// however, it can alternatively be generated by diffing two resource graphs -- in the case of updates to existing
// environments (presumably more common).  The plan contains step objects that can be used to drive a deployment.
type Plan struct {
	ctx       *plugin.Context                  // the plugin context (for provider operations).
	target    *Target                          // the deployment target.
	prev      *Snapshot                        // the old resource snapshot for comparison.
	olds      map[resource.URN]*resource.State // a map of all old resources.
	new       Source                           // the source of new resources.
	analyzers []tokens.QName                   // the analyzers to run during this plan's generation.
}

// NewPlan creates a new deployment plan from a resource snapshot plus a package to evaluate.
//
// From the old and new states, it understands how to orchestrate an evaluation and analyze the resulting resources.
// The plan may be used to simply inspect a series of operations, or actually perform them; these operations are
// generated based on analysis of the old and new states.  If a resource exists in new, but not old, for example, it
// results in a create; if it exists in both, but is different, it results in an update; and so on and so forth.
//
// Note that a plan uses internal concurrency and parallelism in various ways, so it must be closed if for some reason
// a plan isn't carried out to its final conclusion.  This will result in cancelation and reclamation of OS resources.
func NewPlan(ctx *plugin.Context, target *Target, prev *Snapshot, new Source, analyzers []tokens.QName) *Plan {
	contract.Assert(ctx != nil)
	contract.Assert(target != nil)
	contract.Assert(new != nil)

	// Produce a map of all old resources for fast resources.
	olds := make(map[resource.URN]*resource.State)
	if prev != nil {
		for _, oldres := range prev.Resources {
			urn := oldres.URN()
			contract.Assert(olds[urn] == nil)
			olds[urn] = oldres
		}
	}

	return &Plan{
		ctx:       ctx,
		target:    target,
		prev:      prev,
		olds:      olds,
		new:       new,
		analyzers: analyzers,
	}
}

func (p *Plan) Ctx() *plugin.Context                   { return p.ctx }
func (p *Plan) Target() *Target                        { return p.target }
func (p *Plan) Diag() diag.Sink                        { return p.ctx.Diag }
func (p *Plan) Prev() *Snapshot                        { return p.prev }
func (p *Plan) Olds() map[resource.URN]*resource.State { return p.olds }
func (p *Plan) New() Source                            { return p.new }

// Provider fetches the provider for a given resource, possibly lazily allocating the plugins for it.  If a provider
// could not be found, or an error occurred while creating it, a non-nil error is returned.
func (p *Plan) Provider(res resource.Resource) (plugin.Provider, error) {
	return p.ProviderT(res.Type())
}

// ProviderT fetches the provider for a given resource type, possibly lazily allocating the plugins for it.  If a
// provider could not be found, or an error occurred while creating it, a non-nil error is returned.
func (p *Plan) ProviderT(t tokens.Type) (plugin.Provider, error) {
	pkg := t.Package()
	return p.ctx.Host.Provider(pkg)
}