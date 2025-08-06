import tempfile
import platform

# Only apply on Windows
if platform.system() == "Windows":
    # 🔒 Save the original version
    _original_namedtempfile = tempfile.NamedTemporaryFile

    def NamedTemporaryFile(*args, **kwargs):
        kwargs['delete'] = False
        tmp = _original_namedtempfile(*args, **kwargs)  # ✅ use original version
        tmp.close()
        return open(tmp.name, 'rb+')

    # ✅ Monkey-patch
    tempfile.NamedTemporaryFile = NamedTemporaryFile
