import click
import commands.lose_data
import commands.setup
import commands.status


@click.group()
def cli():
    pass


@cli.command()
def status():
    commands.status.main()


@cli.command()
def setup():
    commands.setup.main()


@cli.command()
def lose_data():
    commands.lose_data.main()


if __name__ == "__main__":
    cli()
