use crate::routing::{PipelineRoutes, StageRoute, RouteDecision};

pub fn convert_assignment_to_routes(
    assignment: conveyor_proto::sidecar::PipelineAssignment,
) -> PipelineRoutes {
    let stages = assignment
        .stages
        .into_iter()
        .map(|stage| {
            let decision = match stage.target {
                Some(conveyor_proto::sidecar::stage_assignment::Target::LocalEndpoint(ep)) => {
                    RouteDecision::Local { endpoint: ep }
                }
                Some(conveyor_proto::sidecar::stage_assignment::Target::RemoteSidecar(remote)) => {
                    RouteDecision::Remote {
                        sidecar_id: remote.sidecar_id,
                        endpoint: remote.endpoint,
                    }
                }
                None => RouteDecision::Local {
                    endpoint: String::new(),
                },
            };

            (
                stage.stage_id.clone(),
                StageRoute {
                    stage_id: stage.stage_id,
                    decision,
                },
            )
        })
        .collect();

    PipelineRoutes {
        pipeline_id: assignment.pipeline_id,
        is_local_complete: assignment.is_local_complete,
        stages,
    }
}
