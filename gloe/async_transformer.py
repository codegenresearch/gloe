import copy\nimport traceback\nimport types\nimport uuid\nfrom abc import abstractmethod, ABC\nfrom inspect import Signature\nfrom typing import TypeVar, overload, cast, Any, Callable, Awaitable, Union, Tuple, List\n\nfrom gloe.base_transformer import (\n    TransformerException,\n    BaseTransformer,\n    PreviousTransformer,\n)\nfrom gloe.exceptions import UnsupportedTransformerArgException\n\n__all__ = ["AsyncTransformer"]\n\n_In = TypeVar("_In")\n_Out = TypeVar("_Out")\n_NextOut = TypeVar("_NextOut")\n_Out2 = TypeVar("_Out2")\n_Out3 = TypeVar("_Out3")\n_Out4 = TypeVar("_Out4")\n_Out5 = TypeVar("_Out5")\n_Out6 = TypeVar("_Out6")\n_Out7 = TypeVar("_Out7")\n\nclass AsyncTransformer(BaseTransformer[_In, _Out, "AsyncTransformer"], ABC):\n    def __init__(self):\n        super().__init__()\n        self._graph_node_props: dict[str, Any] = {\n            **self._graph_node_props,\n            "isAsync": True,\n        }\n        self.__class__.__annotations__ = self.transform_async.__annotations__\n\n    @abstractmethod\n    async def transform_async(self, data: _In) -> _Out:\n        """\n        Method to perform the transformation asynchronously.\n\n        Args:\n            data: the incoming data passed to the transformer during the pipeline execution.\n\n        Returns:\n            The outcome data, it means, the result of the transformation.\n        """\n        pass\n\n    def signature(self) -> Signature:\n        return self._signature(AsyncTransformer)\n\n    def __repr__(self):\n        return f"{self.input_annotation} -> ({type(self).__name__}) -> {self.output_annotation}"\n\n    async def __call__(self, data: _In) -> _Out:\n        transformed: _Out | None = None\n        try:\n            transformed = await self.transform_async(data)\n        except TransformerException as e:\n            raise e.internal_exception from e\n        except Exception as e:\n            tb = traceback.extract_tb(e.__traceback__)\n            transformer_frames = [\n                frame\n                for frame in tb\n                if frame.name == self.__class__.__name__ or frame.name == "transform_async"\n            ]\n            if len(transformer_frames) == 1:\n                transformer_frame = transformer_frames[0]\n                exception_message = (\n                    f"\n  "\n                    f'File "{transformer_frame.filename}", line {transformer_frame.lineno}, '\n                    f'in transformer "{self.__class__.__name__}"\n  "">>{transformer_frame.line}"'\n                )\n            else:\n                exception_message = (\n                    f'An error occurred in transformer "{self.__class__.__name__}"'\n                )\n            raise TransformerException(\n                internal_exception=e,\n                raiser_transformer=self,\n                message=exception_message,\n            ) from e\n\n        if isinstance(transformed, _Out):\n            return transformed\n        else:\n            raise NotImplementedError  # pragma: no cover\n\n    def copy(\n        self,\n        transform: Callable[[BaseTransformer, _In], Awaitable[_Out]] | None = None,\n        regenerate_instance_id: bool = False,\n    ) -> "AsyncTransformer[_In, _Out]":\n        copied = copy.copy(self)\n        func_type = types.MethodType\n        if transform is not None:\n            setattr(copied, "transform_async", func_type(transform, copied))\n        if regenerate_instance_id:\n            copied.instance_id = uuid.uuid4()\n        if self.previous is not None:\n            if isinstance(self.previous, tuple):\n                new_previous: List[BaseTransformer] = [\n                    previous_transformer.copy()\n                    for previous_transformer in self.previous\n                ]\n                copied._previous = cast(PreviousTransformer, tuple(new_previous))\n            elif isinstance(self.previous, BaseTransformer):\n                copied._previous = self.previous.copy()\n        copied._children = [\n            child.copy(regenerate_instance_id=True) for child in self.children\n        ]\n        return copied\n\n    @overload\n    def __rshift__(\n        self, next_node: BaseTransformer[_Out, _NextOut, Any]\n    ) -> "AsyncTransformer[_In, _NextOut]":\n        pass\n\n    @overload\n    def __rshift__(\n        self,\n        next_node: Tuple[\n            BaseTransformer[_Out, _NextOut, Any],\n            BaseTransformer[_Out, _Out2, Any],\n        ],\n    ) -> "AsyncTransformer[_In, Tuple[_NextOut, _Out2]]":\n        pass\n\n    @overload\n    def __rshift__(\n        self,\n        next_node: Tuple[\n            BaseTransformer[_Out, _NextOut, Any],\n            BaseTransformer[_Out, _Out2, Any],\n            BaseTransformer[_Out, _Out3, Any],\n        ],\n    ) -> "AsyncTransformer[_In, Tuple[_NextOut, _Out2, _Out3]]":\n        pass\n\n    @overload\n    def __rshift__(\n        self,\n        next_node: Tuple[\n            BaseTransformer[_Out, _NextOut, Any],\n            BaseTransformer[_Out, _Out2, Any],\n            BaseTransformer[_Out, _Out3, Any],\n            BaseTransformer[_Out, _Out4, Any],\n        ],\n    ) -> "AsyncTransformer[_In, Tuple[_NextOut, _Out2, _Out3, _Out4]]":\n        pass\n\n    @overload\n    def __rshift__(\n        self,\n        next_node: Tuple[\n            BaseTransformer[_Out, _NextOut, Any],\n            BaseTransformer[_Out, _Out2, Any],\n            BaseTransformer[_Out, _Out3, Any],\n            BaseTransformer[_Out, _Out4, Any],\n            BaseTransformer[_Out, _Out5, Any],\n        ],\n    ) -> "AsyncTransformer[_In, Tuple[_NextOut, _Out2, _Out3, _Out4, _Out5]]":\n        pass\n\n    @overload\n    def __rshift__(\n        self,\n        next_node: Tuple[\n            BaseTransformer[_Out, _NextOut, Any],\n            BaseTransformer[_Out, _Out2, Any],\n            BaseTransformer[_Out, _Out3, Any],\n            BaseTransformer[_Out, _Out4, Any],\n            BaseTransformer[_Out, _Out5, Any],\n            BaseTransformer[_Out, _Out6, Any],\n        ],\n    ) -> "AsyncTransformer[_In, Tuple[_NextOut, _Out2, _Out3, _Out4, _Out5, _Out6]]":\n        pass\n\n    @overload\n    def __rshift__(\n        self,\n        next_node: Tuple[\n            BaseTransformer[_Out, _NextOut, Any],\n            BaseTransformer[_Out, _Out2, Any],\n            BaseTransformer[_Out, _Out3, Any],\n            BaseTransformer[_Out, _Out4, Any],\n            BaseTransformer[_Out, _Out5, Any],\n            BaseTransformer[_Out, _Out6, Any],\n            BaseTransformer[_Out, _Out7, Any],\n        ],\n    ) -> "AsyncTransformer[_In, Tuple[_NextOut, _Out2, _Out3, _Out4, _Out5, _Out6, _Out7]]":\n        pass\n\n    def __rshift__(self, next_node):\n        if isinstance(next_node, BaseTransformer):\n            return _compose_nodes(self, next_node)\n        elif isinstance(next_node, tuple):\n            if all(isinstance(node, BaseTransformer) for node in next_node):\n                return _compose_nodes(self, next_node)\n            else:\n                unsupported_elem = [\n                    elem for elem in next_node if not isinstance(elem, BaseTransformer)\n                ]\n                raise UnsupportedTransformerArgException(unsupported_elem[0])\n        else:\n            raise UnsupportedTransformerArgException(next_node)\n