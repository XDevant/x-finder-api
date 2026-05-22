from tkinter import Label, Listbox, Entry, Button, Scrollbar, Frame, END
from typing import Callable, Literal
from x_finder.utils.widgets import widgets as widget_list


class TkWidgetMixin:
    def say(self, message: str):
        try:
            self.__getattribute__("message_box").insert(END, message)
        except AttributeError:
            print("Message Box not found")

    def update_label(self, label: str, text: str) -> None:
        try:
            self.__getattribute__(label).config(text=text)
        except AttributeError:
            print(f"Label {label} not found")

    def update_button(self, button: str,
                      color: str = 'grey',
                      default: Literal['normal', 'active', 'disabled'] = 'disabled'
                      ) -> None:
        try:
            self.__getattribute__(button + "_button").config(bg=color, default=default)
        except AttributeError:
            print(f"Button {button} not found")

    def initialize_listbox(self, name: str,
                           parent,
                           widget_options: dict[str, str | int],
                           grid_options: dict[str, str | int | tuple[int,int]],
                           other: dict[str, bool] |None = None)-> None:
        if name.startswith("message"):
            name = "message_box"
        else:
            name = name[:-3]
        self.__setattr__(name, Listbox(parent, **widget_options))
        box = self.__getattribute__(name)
        if "rowspan" in grid_options.keys() and grid_options["rowspan"] > 0:
            box.grid(**grid_options)
        box.bind('<<ListboxSelect>>', lambda e: self.on_select(name,
                                                                   e.widget.curselection()
                                                                   ))
        if not (other and "scrollbar"in other.keys() and not other["scrollbar"]):
            self.__setattr__(f"scroll_v_{name}",
                             Scrollbar(parent, orient="vertical", command=box.yview))
            box['yscrollcommand'] = self.__getattribute__(f"scroll_v_{name}").set
            offset = 1
            options = {**grid_options}
            if "columnspan" in grid_options.keys():
                span = grid_options["columnspan"]
                if isinstance(span, int):
                    offset = span
            if "column" in grid_options.keys():
                options["column"] = offset + grid_options["column"]
            options['padx'] = (0, 8)
            self.__getattribute__(f"scroll_v_{name}").grid(**options)

        if other and "scrollbar_h"in other.keys() and other["scrollbar_h"]:
            self.__setattr__(f"scroll_h_{name}",
                             Scrollbar(parent, orient="horizontal", command=box.xview))
            box['xscrollcommand'] = self.__getattribute__(f"scroll_h_{name}").set
            offset = 1
            if "rowspan" in grid_options.keys():
                span = grid_options["rowspan"]
                if isinstance(span, int):
                    offset = span
            if "row" in grid_options.keys():
                grid_options["row"] = offset + grid_options["row"]
            self.__getattribute__(f"scroll_h_{name}").grid(**grid_options)

    def initialize_frame(self, name: str,
                         parent,
                         widget_options: dict[str, str | int],
                         grid_options: dict[str, str | int],
                         other: dict[str, bool] |None = None) -> None:
        self.__setattr__(name, Frame(parent, **widget_options))
        self.__getattribute__(name).grid(**grid_options)

    def initialize_label(self, name: str,
                         parent,
                         widget_options: dict[str, str | int],
                         grid_options: dict[str, str | int],
                         other: dict[str, bool] |None = None) -> None:
        if name:
            if not "text" in widget_options.keys() or not widget_options["text"]:
                text = " ".join(name.split("_")[1:-1]).capitalize()
                widget_options = {**widget_options, "text": text}
            self.__setattr__(name, Label(parent, **widget_options))
            self.__getattribute__(name).grid(**grid_options)

    def initialize_button(self,
                          name: str,
                          parent, widget_options: dict[str, str | int | Callable],
                          grid_options: dict[str, str | int],
                          other: dict[str, bool] | None = None) -> None:
        if name:
            if not "text" in widget_options.keys() or not widget_options["text"]:
                text = " ".join(name.split("_")[:-1]).title()
                widget_options = {**widget_options, "text": text}
            if not "command" in widget_options.keys():
                command_name = name.split("_button")[0]
                try:
                    command = self.__getattribute__(command_name)
                    widget_options = {**widget_options, "command": command}
                except AttributeError:
                    print(f"{command_name} command not found")
                    command = lambda e: None
                widget_options = {**widget_options, "command": command}
            self.__setattr__(name, Button(parent, **widget_options))
            self.__getattribute__(name).grid(**grid_options)

    def initialize_entry(self,
                         name: str,
                         parent,
                         widget_options: dict[str, str | int | Callable],
                         grid_options: dict[str, str | int],
                         other: dict[str, bool] |None = None) -> None:
        self.__setattr__(name, Entry(parent, **widget_options))
        entry = self.__getattribute__(name)
        entry.grid(**grid_options)
        command_name = f"execute_{name}"
        try:
            entry.bind('<Return>', self.__getattribute__(command_name))
        except AttributeError:
            print(f"{command_name} command not found")

    def initialize_widgets(self) -> None:
        for key in widget_list.keys():
            widget_default = widget_list[key]["widget"]
            grid_default = widget_list[key]["grid"]
            widgets = widget_list[key]["widgets"]
            for widget in widgets:
                self.initialize_widget(key, widget, widget_default, grid_default)

    def initialize_widget(self,
                          name: str,
                          widget: dict[str, str],
                          widget_default: dict[str, str | int],
                          grid_default: dict[str, str | int]) -> None:
        name = name.strip('!').lower()
        widget_name = f"{widget['name']}_{name}"
        parent_name = widget['parent']
        if name == "label":
            widget_name = parent_name + "_" + widget_name
        if widget["name"] != 'root':
            parent_name += "_frame"
        other = None
        if "other" in widget.keys():
            other = widget["other"]
        self.__getattribute__(f"initialize_{name}")(widget_name,
                                                    self.__getattribute__(parent_name),
                                                    {**widget_default, **widget["widget"]},
                                                    {**grid_default, **widget["grid"]},
                                                    other=other)

    def on_select(self, list_name: str, curselection: list[int]) -> None:
        pass
