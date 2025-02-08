from pygments.style import Style\nfrom pygments.token import (\n    Keyword,\n    Name,\n    Comment,\n    String,\n    Error,\n    Number,\n    Operator,\n    Generic,\n    Token,\n    Whitespace,\n)\n\n\nclass GloeStyle(Style):\n    name = "gloe"\n    background_color = "#202020"\n    highlight_color = "#404040"\n    line_number_color = "#aaaaaa"\n\n    styles = {\n        Token: "#d0d0d0",\n        Whitespace: "#666666",\n        Comment: "italic #ababab",\n        Comment.Preproc: "noitalic bold #ff3a3a",\n        Comment.Special: "noitalic bold #e50808 bg:#520000",\n        Keyword: "bold #45df9a",\n        Keyword.Pseudo: "nobold",\n        Operator.Word: "bold #45df9a",\n        String: "#6ad7ca",\n        String.Other: "#6ad7ca",\n        Number: "#51b2fd",\n        Name.Builtin: "#2fbccd",\n        Name.Variable: "#40ffff",\n        Name.Constant: "#40ffff",\n        Name.Class: "underline #14c8ef",\n        Name.Function: "#14c8ef",\n        Name.Namespace: "underline #14c8ef",\n        Name.Exception: "#bbbbbb",\n        Name.Tag: "bold #45df9a",\n        Name.Attribute: "#bbbbbb",\n        Name.Decorator: "#6ad7ca",\n        Generic.Heading: "bold #ffffff",\n        Generic.Subheading: "underline #ffffff",\n        Generic.Deleted: "#ff3a3a",\n        Generic.Inserted: "#589819",\n        Generic.Error: "#ff3a3a",\n        Generic.Emph: "italic",\n        Generic.Strong: "bold",\n        Generic.EmphStrong: "bold italic",\n        Generic.Prompt: "#aaaaaa",\n        Generic.Output: "#cccccc",\n        Generic.Traceback: "#ff3a3a",\n        Error: "bg:#e3d2d2 #a61717",\n    }\n