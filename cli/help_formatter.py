import argparse


class HelpFormatter(argparse.HelpFormatter):
    def add_usage(self, usage, actions, groups, prefix=None):
        if prefix is None:
            prefix = "Usage: "
        return super().add_usage(usage, actions, groups, prefix)

    def start_section(self, heading):
        if heading is not None:
            heading = heading.capitalize() + ":"
        super(HelpFormatter, self).start_section(heading)
