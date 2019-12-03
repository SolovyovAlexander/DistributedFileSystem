To run client:
```
py Client.py // -- win --
python3 Client.py // -- UNIX --
```
Client used Tkinter GUI library, which is provided in default python interpreter

First listbox is a list of directories in current directory<br/>
Second listbox is a list of files in current directory<br/>

Red INIT [RESET] button will remove of files from file-tree and all files from storage servers `except` root folder<br/><br/>
DOUBLE-CLICK on item in DIRECTORY LISTBOX (first listbox) will enter the directory
(calls `DIR_OPEN + DIR_READ` calls for target directory)

BUTTON `<-------` calls `DIR_OPEN + DIR_READ` calls for previous directory <br/>

TO `MOVE` OR `COPY` file you need to "buffer" it first.<br/>
--- EXAMPLE ---<br/>
If you want to move `file.kek` from `root` to `root/lol/kek`, first enter in 'root',
select `file.kek` in files listbox, click BUFFER BUTTON, navigate to `root/lol/kek` and press `MOVE FILE HERE`
