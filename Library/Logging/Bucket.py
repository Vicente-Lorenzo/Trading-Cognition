from typing import Union, TextIO
# import credentials_wrapper as cw

from Library.Logging.Logging import VerboseLevel
from Library.Logging.File import FileLoggingAPI
from Library.Utility.Path import inspect_path

class BucketLoggingAPI(FileLoggingAPI):

    _bucket_name_: str = None
    _bucket_link: str = None
    _bucket_ = None

    @classmethod
    def _setup_class_(cls) -> None:
        super()._setup_class_()
        cls.set_verbose_level(VerboseLevel.Debug, default=True)
        cls.set_bucket_name("lisbonTradingData")
        cls.set_bucket_link("https://jupyterhub.cib.echonet/services/s3viewer/bucket/lisbonTRADINGDATA/")

    @classmethod
    def set_bucket_name(cls, bucket_name: str) -> None:
        with cls._class_lock_:
            if cls.is_entered(): return
            cls._bucket_name_ = bucket_name

    @classmethod
    def set_bucket_link(cls, bucket_link: str) -> None:
        with cls._class_lock_:
            if cls.is_entered(): return
            cls._bucket_link = bucket_link

    @classmethod
    def get_file_hyperlink(cls) -> Union[str, None]:
        with cls._class_lock_:
            if not cls._bucket_name_: return super().get_file_hyperlink()
            return cls._bucket_link + inspect_path(cls._file_path_)

    @classmethod
    def _init_(cls):
        return cw.s3.init_connection(cls._bucket_name_)

    @classmethod
    def _exists_(cls) -> bool:
        return cls._bucket_.exists(inspect_path(cls._dir_path_))

    @classmethod
    def _mkdir_(cls) -> None:
        cls._bucket_.mkdirs(inspect_path(cls._dir_path_), exist_ok=True)

    @classmethod
    def _open_(cls) -> TextIO:
        return cls._bucket_.open(inspect_path(cls._file_path_), "w")

    @classmethod
    def _enter_(cls):
        cls._bucket_ = cls._init_()
        super()._enter_()