from pygments.style import Style
from pygments.token import (
    Keyword,
    Name,
    Comment,
    String,
    Error,
    Number,
    Operator,
    Generic,
    Token,
    Whitespace,
)


class GloeDarkStyle(Style):
    name = "gloe-dark"
    background_color = "#202020"
    highlight_color = "#404040"
    line_number_color = "#aaaaaa"

    styles = {
        Token: "#d0d0d0",
        Whitespace: "#666666",
        Comment: "italic #ababab",
        Comment.Preproc: "noitalic bold #ff3a3a",
        Comment.Special: "noitalic bold #e50808 bg:#520000",
        Keyword: "bold #45df9a",
        Keyword.Pseudo: "nobold",
        Operator.Word: "bold #45df9a",
        String: "#6ad7ca",
        String.Other: "#6ad7ca",
        Number: "#51b2fd",
        Name.Builtin: "#2fbccd",
        Name.Variable: "#40ffff",
        Name.Constant: "#40ffff",
        Name.Class: "underline #14c8ef",
        Name.Function: "#14c8ef",
        Name.Namespace: "underline #14c8ef",
        Name.Exception: "#bbbbbb",
        Name.Tag: "bold #45df9a",
        Name.Attribute: "#bbbbbb",
        Name.Decorator: "#6ad7ca",
        Generic.Heading: "bold #ffffff",
        Generic.Subheading: "underline #ffffff",
        Generic.Deleted: "#ff3a3a",
        Generic.Inserted: "#589819",
        Generic.Error: "#ff3a3a",
        Generic.Emph: "italic",
        Generic.Strong: "bold",
        Generic.EmphStrong: "bold italic",
        Generic.Prompt: "#aaaaaa",
        Generic.Output: "#cccccc",
        Generic.Traceback: "#ff3a3a",
        Error: "bg:#e3d2d2 #a61717",
    }


class GloeLightStyle(Style):
    name = "gloe-light"
    background_color = "#ffffff"
    highlight_color = "#404040"
    line_number_color = "#555555"

    styles = {
        Token: "#000000",
        Whitespace: "#bbbbbb",
        Comment: "italic #888888",
        Comment.Preproc: "noitalic bold #4a90d9",
        Comment.Special: "noitalic bold #ff4500 bg:#ffffcc",
        Keyword: "bold #008000",
        Keyword.Pseudo: "nobold",
        Operator.Word: "bold #008000",
        String: "#a31515",
        String.Other: "#a31515",
        Number: "#0000ff",
        Name.Builtin: "#008000",
        Name.Variable: "#0000ff",
        Name.Constant: "#0000ff",
        Name.Class: "underline #0000ff",
        Name.Function: "#0000ff",
        Name.Namespace: "underline #0000ff",
        Name.Exception: "#8b0000",
        Name.Tag: "bold #008000",
        Name.Attribute: "#8b0000",
        Name.Decorator: "#a31515",
        Generic.Heading: "bold #000000",
        Generic.Subheading: "underline #000000",
        Generic.Deleted: "#a00000",
        Generic.Inserted: "#00a000",
        Generic.Error: "#a00000",
        Generic.Emph: "italic",
        Generic.Strong: "bold",
        Generic.EmphStrong: "bold italic",
        Generic.Prompt: "#555555",
        Generic.Output: "#888888",
        Generic.Traceback: "#a00000",
        Error: "bg:#ffcccc #a00000",
    }