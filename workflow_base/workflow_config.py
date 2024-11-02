import logging
from abc import ABC, abstractmethod
from typing import Dict

from .processor import ProcessorConfig

logger = logging.getLogger(__name__)


class WorkflowConfigBase(ABC):
    @property
    @abstractmethod
    def processors(self) -> Dict[str, ProcessorConfig]:
        """Return a dictionary of processor configurations."""
        pass

    def validate(self) -> None:
        """Validate queues used in processors and check for orphaned queues."""
        # Collect all input and output queues used by processors
        all_inputs = {p.input_queue for p in self.processors.values()}
        all_outputs = {q for p in self.processors.values() for q in p.output_queues}

        # Check for orphaned queues (output queues that are not used as inputs)
        orphaned = all_outputs - all_inputs - {""}
        if orphaned:
            raise ValueError(f"Orphaned output queues: {orphaned}")

        logger.info("All queues are valid.")
