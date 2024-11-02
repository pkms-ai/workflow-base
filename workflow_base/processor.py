import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Callable, Coroutine, Dict, List, Optional, Tuple, Type

from aio_pika.abc import AbstractIncomingMessage

logger = logging.getLogger(__name__)


class BaseProcessor(ABC):
    """Abstract base class for processors, defining the main processing and error handling methods,
    and enforcing queue settings as abstract properties."""

    def __init__(
        self, name: str, input_queue: str, output_queues: List[str], error_queue: str
    ):
        self.name = name
        self.input_queue = input_queue
        self.output_queues = output_queues
        self.error_queue = error_queue

    @abstractmethod
    async def process_content(
        self, content: Dict[str, Any]
    ) -> Tuple[str, Dict[str, Any]]:
        """Process content and return (next_queue, processed_content)"""
        pass

    @property
    def handle_error(
        self,
    ) -> Optional[
        Callable[
            [Exception, Optional[Dict[str, Any]], AbstractIncomingMessage],
            Coroutine[Any, Any, None],
        ]
    ]:
        """Optional error handler; defaults to None if not overridden."""
        return None


@dataclass
class ProcessorConfig:
    name: str
    input_queue: str
    output_queues: List[str]  # Now stores route names instead of queue names
    implementation: Type[BaseProcessor]
    error_queue: str = "error_queue"
