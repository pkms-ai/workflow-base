from workflow_base import BaseProcessor

from .workflow_config import WorkflowConfigBase


class WorkflowManager:
    def __init__(self, config: WorkflowConfigBase):
        self.config = config
        self.config.validate()

        # # Create Route enum dynamically
        # self.Route = Enum('Route', {name: name for name in self.routes.keys()})
        #
        # # Build processor configs
        # for name, cfg in self.config["processors"].items():
        #     self.processors[name] = ProcessorConfig(
        #         name=name,
        #         input_queue=self._get_queue_name(cfg["input"]),
        #         output_routes=cfg["outputs"],
        #         implementation=cfg["implementation"]
        #     )

    # def _get_queue_name(self, route: str) -> str:
    #     # """Converts route name to queue name"""
    #     # return self.routes.get(route, route)  # Fallback to direct queue name if not a route
    #     raise NotImplementedError
    #
    # def get_queue_for_route(self, route: str) -> str:
    #     """Gets the queue name for a route"""
    #     # return self.routes[route]
    #     raise NotImplementedError

    # def get_processor_class(self, implementation: str) -> Type["BaseProcessor"]:
    #     """Dynamically imports and returns the processor class"""
    #     module_path, class_name = implementation.rsplit(".", 1)
    #     module = importlib.import_module(module_path)
    #     return getattr(module, class_name)

    def create_processor(self, name: str) -> "BaseProcessor":
        if name not in self.config.processors:
            raise ValueError(f"Processor {name} not found in config.")
        processor_config = self.config.processors[name]

        processor_class = processor_config.implementation
        return processor_class(
            name=processor_config.name,
            input_queue=processor_config.input_queue,
            output_queues=processor_config.output_queues,
            error_queue=processor_config.error_queue,
        )
