# -*- coding: utf-8 -*-
from __future__ import division, absolute_import, unicode_literals

import click

from oar.lib import config
from oar.lib.utils import cached_property

from ..helpers import (make_pass_decorator, Context,
                       config_default_value, load_configuration_file)


CONTEXT_SETTINGS = dict(auto_envvar_prefix='oar_migrate',
                        help_option_names=['-h', '--help'])


class CreateContext(Context):

    @cached_property
    def current_db(self):
        from oar.lib import db
        db._cache["uri"] = self.current_db_url
        return db


pass_context = make_pass_decorator(CreateContext)


def validate_db_admin_auth(ctx, param, value):
    is_local = ctx.params.get('db_is_local', False)
    if value is None and not is_local:
        raise click.BadParameter('mandatory parameter. Set "--db-is-local" '
                                 'flag to use the local admin account')


@click.command(context_settings=CONTEXT_SETTINGS)
@click.version_option()
@click.option('-c', '--conf', callback=load_configuration_file,
              type=click.Path(writable=False, readable=False, exists=True,
                              file_okay=True, resolve_path=True),
              help="Use a different OAR configuration file.",
              default=config.DEFAULT_CONFIG_FILE, show_default=True)
@click.option('-y', '--force-yes', is_flag=True, default=False,
              help="Never prompts for user intervention")
@click.option('--db-host', help='Set the database hostname',
              default=config_default_value('DB_HOSTNAME'), required=True)
@click.option('--db-port', help='Set the database port',
              default=config_default_value('DB_PORT'), required=True)
@click.option('--db-user', help='Set the database user',
              default=config_default_value('DB_BASE_LOGIN'), required=True)
@click.option('--db-pass', help='Set the database password',
              default=config_default_value('DB_BASE_PASSWD'), required=True)
@click.option('--db-name', help='Set the database name',
              default=config_default_value('DB_BASE_NAME'), required=True)
@click.option('--db-ro-user', help='Set the read-only database user',
              default=config_default_value('DB_BASE_LOGIN_RO'), required=True)
@click.option('--db-ro-pass', help='Set the read-only database pass',
              default=config_default_value('DB_BASE_PASSWD_RO'), required=True)
@click.option('--db-is-local', is_flag=True, default=False,
              help='The database is local, the script can use the local admin '
                   'account to execute command')
@click.option('--db-admin-user', help='Set the database admin user',
              callback=validate_db_admin_auth)
@click.option('--db-admin-pass', help='Set the database admin password',
              callback=validate_db_admin_auth)
@click.option('--verbose', is_flag=True, default=False,
              help="Enables verbose output.")
@click.option('--debug', is_flag=True, default=False,
              help="Enables debug mode.")
@pass_context
def cli(ctx, **kwargs):
    """Create OAR database."""
    ctx.update_options(**kwargs)
    ctx.configure_log()
    ctx.confirm("Continue to create your database?", default=True)
    # TODO: create all database/roles
