"""Initialize app."""
from flask import Flask


def create_app():
    """Construct the core application."""
    app = Flask(__name__,
                instance_relative_config=False)
    app.config.from_object('config.Config')

    with app.app_context():
        # Import Dash application
        from .dash_application import app
        dash_app = app.Add_Dash(app)

        return dash_app
