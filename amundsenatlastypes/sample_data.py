from abc import ABC, abstractmethod


class SampleData(ABC):
    @abstractmethod
    def _create(self, *args, **kwargs):
        """
        Method for creating sample data.

        :return:
        """
        pass

    @abstractmethod
    def _initialize(self):
        """
        Method for initializing all entity types required to load sample data.

        :return:
        """
        pass

    def create(self, *args, **kwargs):
        self._initialize()
        self._create(*args, **kwargs)

        pass
