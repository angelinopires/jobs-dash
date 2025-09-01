"""
Base Manager Class for Shared Patterns

This module provides a base class for managers that need:
- Singleton pattern
- Registry pattern
- Configuration management
- Error handling with fallbacks

For front-end developers learning Python:
- This is like creating a base class in TypeScript/JavaScript
- The singleton pattern is like a global context in React
- The registry pattern is like a service container in Angular
"""

import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, Generic, Optional, TypeVar

# Set up logging
logger = logging.getLogger(__name__)

# Type variable for the managed item type
T = TypeVar("T")


class BaseManager(ABC, Generic[T]):
    """
    Base class for managers that need singleton and registry patterns.

    This eliminates code duplication between different manager types.
    Think of it like a base class in TypeScript or a mixin in JavaScript.
    """

    def __init__(self, manager_name: str):
        """
        Initialize the base manager

        Args:
            manager_name: Name of this manager instance
        """
        self.manager_name = manager_name
        self._registry: Dict[str, T] = {}
        self._initialized = False

        logger.debug(f"Base manager '{manager_name}' initialized")

    @abstractmethod
    def _create_item(self, name: str, **kwargs: Any) -> T:
        """
        Create a new item for the registry.
        Must be implemented by subclasses.

        Args:
            name: Name of the item to create
            **kwargs: Additional creation parameters

        Returns:
            T: The created item
        """
        pass

    def get_item(self, name: str, **kwargs: Any) -> T:
        """
        Get or create an item from the registry (singleton pattern)

        Args:
            name: Name of the item
            **kwargs: Creation parameters if item doesn't exist

        Returns:
            T: The item instance
        """
        if name not in self._registry:
            logger.debug(f"Creating new item '{name}' in {self.manager_name}")
            self._registry[name] = self._create_item(name, **kwargs)

        return self._registry[name]

    def get_all_items(self) -> Dict[str, T]:
        """
        Get all items in the registry

        Returns:
            Dict[str, T]: Copy of all registered items
        """
        return self._registry.copy()

    def clear_registry(self) -> None:
        """Clear all items from the registry"""
        self._registry.clear()
        logger.debug(f"Cleared registry for {self.manager_name}")

    def has_item(self, name: str) -> bool:
        """
        Check if an item exists in the registry

        Args:
            name: Name of the item

        Returns:
            bool: True if item exists
        """
        return name in self._registry

    def remove_item(self, name: str) -> bool:
        """
        Remove an item from the registry

        Args:
            name: Name of the item to remove

        Returns:
            bool: True if item was removed, False if not found
        """
        if name in self._registry:
            del self._registry[name]
            logger.debug(f"Removed item '{name}' from {self.manager_name}")
            return True
        return False


class SingletonManager(BaseManager[T]):
    """
    Base class for singleton managers.

    This is like a global context provider in React or a service in Angular.
    """

    def __init__(self, manager_name: str):
        super().__init__(manager_name)
        self._singleton_instance: Optional[T] = None

    def get_singleton(self, **kwargs: Any) -> T:
        """
        Get or create the singleton instance

        Args:
            **kwargs: Creation parameters if instance doesn't exist

        Returns:
            T: The singleton instance
        """
        if self._singleton_instance is None:
            logger.debug(f"Creating singleton instance for {self.manager_name}")
            self._singleton_instance = self._create_item(self.manager_name, **kwargs)

        return self._singleton_instance

    def reset_singleton(self) -> None:
        """Reset the singleton instance"""
        self._singleton_instance = None
        logger.debug(f"Reset singleton instance for {self.manager_name}")


# Global manager registry for different manager types
_managers: Dict[str, BaseManager] = {}


def get_manager(manager_type: str, manager_name: str) -> BaseManager:
    """
    Get or create a manager instance

    Args:
        manager_type: Type of manager (e.g., 'environment', 'circuit_breaker')
        manager_name: Name of the manager

    Returns:
        BaseManager: The manager instance
    """
    key = f"{manager_type}_{manager_name}"
    if key not in _managers:
        logger.debug(f"Creating new manager: {key}")
        # This would be implemented by specific manager classes
        raise NotImplementedError(f"Manager type '{manager_type}' not implemented")

    return _managers[key]


def register_manager(manager_type: str, manager_name: str, manager: BaseManager) -> None:
    """
    Register a manager instance

    Args:
        manager_type: Type of manager
        manager_name: Name of the manager
        manager: Manager instance
    """
    key = f"{manager_type}_{manager_name}"
    _managers[key] = manager
    logger.debug(f"Registered manager: {key}")


def get_all_managers() -> Dict[str, BaseManager]:
    """
    Get all registered managers

    Returns:
        Dict[str, BaseManager]: All registered managers
    """
    return _managers.copy()
