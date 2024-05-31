import argparse


class HelpFormatter(argparse.HelpFormatter):
    def __init__(self, prog, indent_increment=4, max_help_position=24, width=None):
        super().__init__(prog, indent_increment, max_help_position, width)
        self._description = None

    def add_usage(self, usage, actions, groups, prefix=None):
        if prefix is None:
            prefix = "Usage: "
        return super().add_usage(usage, actions, groups, prefix)

    def start_section(self, heading):
        if heading is not None:
            heading = heading.capitalize()
        super().start_section(heading)

    def format_help(self):
        help_text = super().format_help()
        if self._description:
            description_heading = "Description:\n"
            formatted_description = self._format_text(self._description) + "\n"
            help_text = help_text.replace(
                self._format_text(self._description),
                description_heading + formatted_description,
            )
        return help_text

    def set_description(self, description):
        self._description = description
