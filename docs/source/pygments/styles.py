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
    highlight_color = "#e0e0e0"
    line_number_color = "#333333"

    styles = {
        Token: "#333333",
        Whitespace: "#999999",
        Comment: "italic #666666",
        Comment.Preproc: "noitalic bold #cc0000",
        Comment.Special: "noitalic bold #cc0000 bg:#ffcccc",
        Keyword: "bold #008800",
        Keyword.Pseudo: "nobold",
        Operator.Word: "bold #008800",
        String: "#0066cc",
        String.Other: "#0066cc",
        Number: "#0000cc",
        Name.Builtin: "#0066cc",
        Name.Variable: "#0000cc",
        Name.Constant: "#0000cc",
        Name.Class: "underline #0066cc",
        Name.Function: "#0066cc",
        Name.Namespace: "underline #0066cc",
        Name.Exception: "#cc0000",
        Name.Tag: "bold #008800",
        Name.Attribute: "#cc0000",
        Name.Decorator: "#0066cc",
        Generic.Heading: "bold #000000",
        Generic.Subheading: "underline #000000",
        Generic.Deleted: "#cc0000",
        Generic.Inserted: "#00cc00",
        Generic.Error: "#cc0000",
        Generic.Emph: "italic",
        Generic.Strong: "bold",
        Generic.EmphStrong: "bold italic",
        Generic.Prompt: "#333333",
        Generic.Output: "#666666",
        Generic.Traceback: "#cc0000",
        Error: "bg:#ffcccc #cc0000",
    }