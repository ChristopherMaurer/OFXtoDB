OFXtoDB Installation Notes
This is a Windows release of the program.  I'll work on a Linux release, but for now you can download the *.py files and run "pyinstaller --onefile OFXtoDB.py" yourself to build an executable for that platform if you have Python installed.

You can store the files in this release together in any directory you choose.

The shortcut is actually a shortcut to CMD.EXE which runs OFXtoDB in a window that does not close so you can see the Add/Update statistics at the end.  To use it, you must alter the working directory to point to where the executable resides. Then you can put it on the Desktop or wherever you like, so you can invoke the program manually by dragging and dropping your .qfx file onto this shortcut.  This is the most convenient way on Windows, where you log into your bank/credit card/brokerage company's website, download Quicken format activity, open the browser download directory, and drag the file and drop it on top of the OFXtoDB shortcut on the desktop.

The OFXtoDB.ini file can reside either in the working directory or in %APPDATA%\OFXtoDB.