import os
import sys
from abc import abstractmethod
from typing import (
    Annotated,
    Generic,
    List,
    Literal,
    Optional,
    TypeAlias,
    TypeVar,
    get_args,
    get_origin,
)

import pandas as pd
from expression import Option
from expression.core.option import of_optional
from pydantic import GetPydanticSchema, PlainValidator
from pydantic_core import SchemaValidator, core_schema
from pydantic_settings import BaseSettings

from contek_pyutils.config import get_config_path, load_yaml_with_import
from contek_pyutils.func.core import none_or
from contek_pyutils.singleton import SingletonABC

T = TypeVar("T", bound=BaseSettings)


class GlobalCfg(Generic[T], metaclass=SingletonABC):
    def __init__(self):
        self.config: Optional[T] = None  # type: ignore
        self.config_path: Optional[str] = None
        self.raw_config: Optional[dict] = None
        self.load_config()

    def load_config(self):
        self.config_path: Optional[str] = get_config_path()
        self.raw_config: Optional[dict] = load_yaml_with_import(self.config_path) | {"config_path": self.config_path}

    @property
    @abstractmethod
    def config_type(self) -> type[T]:
        raise NotImplementedError

    @property
    def default_context(self) -> dict:
        return {}

    def get(
        self,
        reload=False,
        context=None,
        cli_command_to_env: List | Literal["all"] = "all",
    ) -> T:
        if reload:
            self.load_config()
        if self.config is None:
            config_type = self.config_type
            env_prefix = config_type.model_config.get("env_prefix", "")
            i = 1
            cli_command_to_env = none_or(cli_command_to_env, [])
            while i < len(sys.argv):
                arg = sys.argv[i]
                if arg.startswith("--") and (cli_command_to_env == "all" or arg[2:] in cli_command_to_env):
                    os.environ[f"{env_prefix}{arg[2:]}"] = sys.argv[i + 1]
                    i += 2
                else:
                    i += 1

            config_type.settings_customise_sources = classmethod(  # type: ignore
                lambda cls, setting_cls, init_settings, env_settings, dotenv_settings, file_secret_settings: (
                    env_settings,
                    init_settings,
                    dotenv_settings,
                    file_secret_settings,
                )
            )
            self.config = config_type.model_validate(self.raw_config, context=none_or(context, self.default_context))
        return self.config


def option_schema(tp, handler):
    args = get_args(tp)
    origin = get_origin(tp) if args else tp
    arg_schema = handler(args[0]) if args else core_schema.any_schema()
    instance_schema = core_schema.no_info_after_validator_function(
        lambda v: v.map(lambda vv: SchemaValidator(arg_schema).validate_python(vv)),
        core_schema.is_instance_schema(cls=origin),
    )
    optional_schema = core_schema.nullable_schema(arg_schema)
    non_instance_scheme = core_schema.no_info_after_validator_function(lambda v: of_optional(v), optional_schema)
    return core_schema.union_schema(
        [instance_schema, non_instance_scheme],
        serialization=core_schema.plain_serializer_function_ser_schema(
            lambda v: v.default_value(None), return_schema=optional_schema
        ),
    )


T = TypeVar("T")
PydOption: TypeAlias = Annotated[Option[T], GetPydanticSchema(option_schema)]
PydPdtd: TypeAlias = Annotated[pd.Timedelta, PlainValidator(lambda v: pd.Timedelta(v))]
__all__ = ["GlobalCfg", "PydOption", "PydPdtd"]
