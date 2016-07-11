__author__ = 'lucile'
# -*- coding: = utf-8 -*-

#from app.Recepteur.nda import nda
from app.Recepteur.nsa import nsa
import re
from flask import Flask, jsonify
from flask import request
from flask.ext.log import Logging
from flask.ext.sqlalchemy import SQLAlchemy
from flask.ext.security import Security, SQLAlchemyUserDatastore, \
    UserMixin, RoleMixin, login_required , utils, roles_accepted,confirmable,registerable
from flask_mail import Mail
from flask.ext.security.utils import send_mail, md5, url_for_security, get_token_status,\
    config_value
from flask.ext.security.signals import user_confirmed, confirm_instructions_sent
from werkzeug.datastructures import ImmutableMultiDict as IM

app = Flask(__name__)
app.config['FLASK_LOG_LEVEL'] = 'INFO'
flask_log = Logging(app)
app.logger.debug('Testing a debug message')
app.config['DEBUG'] = True
app.config['SECRET_KEY'] = 'zijdle,t7ie1'
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///rsdb.db'
app.config['SECURITY_URL_PREFIX'] = "/bck"
app.config['MAIL_SERVER'] = 'smtp.obs-nancay.fr'
#app.config['CONFIRMABLE'] = True
#app.config['REGISTERABLE'] = True
app.config['SECURITY_EMAIL_SENDER'] = 'admin@ambari-rsdb.obs-nancay.fr'
app.config['SECURITY_RECOVERABLE'] = True
app.config['SECURITY_REGISTERABLE'] = True
app.config['SECURITY_CONFIRMABLE'] = True
app.config['SECURITY_PASSWORD_HASH'] ='sha256_crypt'
app.config['SECURITY_PASSWORD_SALT'] = 'encore'
app.config['SECURITY_UNAUTHORIZED_VIEW'] = 'bck/login'
#app.config['SECURITY_CONFIRM_URL' ] = '/../confirm'
#app.config['SECURITY_LOGIN_URL'] = '/bck/login'
#app.config['SECURITY_LOGOUT_URL'] ='/bck/logout'
#app.config['SECURITY_REGISTER_URL'] ='/bck/register'
#app.config['SECURITY_RESET_URL'] ='/bck/reset'
#app.config['SECURITY_CHANGE_URL'] ='/bck/change'

#app.config['SECURITY_POST_LOGIN_VIEW'] ='/bck/'
#app.config['SECURITY_POST_LOGOUT_VIEW'] ='/bck/'
#app.config['MAIL_PORT'] = 465
#app.config['MAIL_USE_SSL'] = True
#app.config['MAIL_USERNAME'] = 'username'
#app.config['MAIL_PASSWORD'] = 'password'
mail = Mail(app)
db = SQLAlchemy(app)

recepteurs = {
    #'Nda':nda,
    #'Nrh':nrh,
    'Nsa':nsa
}
# Define models
roles_users = db.Table('roles_users',
        db.Column('user_id', db.Integer(), db.ForeignKey('user.id')),
        db.Column('role_id', db.Integer(), db.ForeignKey('role.id')))

class Role(db.Model, RoleMixin):
    id = db.Column(db.Integer(), primary_key=True)
    name = db.Column(db.String(80), unique=True)
    description = db.Column(db.String(255))

class User(db.Model, UserMixin):
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(255), unique=True)
    password = db.Column(db.String(255))
    active = db.Column(db.Boolean())
    confirmed_at = db.Column(db.DateTime())
    #token = db.Column(db.String(255))
    roles = db.relationship('Role', secondary=roles_users,
                            backref=db.backref('users', lazy='dynamic'))
# Setup Flask-Security
user_datastore = SQLAlchemyUserDatastore(db, User, Role)
security = Security(app, user_datastore,send_confirmation_form=True)

# Create a user to test with
"""
@app.before_first_request
def role():
    user_datastore.create_role(name="view", description="")
    user_datastore.create_role(name="admin", description="")
    user_datastore.create_role(name="research", description="")
    db.session.commit()
"""

@app.route('/confirm/<string:token>',methods=['POST'])
def confirm(token):
    expired, invalid, user= utils.get_token_status(token, "confirm", max_age="CONFIRM_EMAIL", return_data=False)
    if expired or invalid:
        response = {
            "status": False,
            "message": "Not Confirmed",
            "route":"register"
        }
    else:
        if user:

            user_datastore.activate_user(user)
            confirmable.confirm_user(user)
            db.session.commit()
            response = {
                "status": True,
                "message": "Confirmed",
                "route":"login",
                "confirmed_at":user.confirmed_at

            }
        else:
            response = {
                "status": False,
                "message": "Not Comfirmed",
                "route":"comfirm"

            }

    return jsonify(response )


@app.route('/register',methods=['POST'])
def register():

    json = request.get_json()
    mail = json["mail"]
    password = json["password"]
    #mail = request.args.get("mail")
    #password = request.args.get("password")

    if user_datastore.get_user(mail):
        response = {
            "status": True,
            "message": "Allready registered",
            "route":"login",
            "user":mail
        }
    else:
        #p = re.compile(ur'^((?=\S*?[A-Z])(?=\S*?[a-z])(?=\S*?[0-9]).{6,})\S$')


        valid_p = re.search('^((?=\S*?[A-Z])(?=\S*?[a-z])(?=\S*?[0-9]).{6,})\S$', password)
        if valid_p:
            #p = re.compile(ur'^[\w\d](\.?[\w\d_-])*@[\w\d]+\.([\w]{1,6}\.)?[\w]{2,6}$')


            valid_m = re.findall('^[\w\d_-](\.?[\w\d_-])*@[\w\d_-]+\.([\w]{1,6}\.)?[\w]{2,6}$', mail)
            if valid_m:
                p = utils.encrypt_password(password)
                u = user_datastore.create_user(email=mail, password=p,roles=["view"],confirmed_at=None)

                db.session.commit()
                token = confirmable.generate_confirmation_token(u)
                confirmation_link = "http://ambari-rsdb.obs-nancay.fr/#!/confirm/" + token

                send_mail(config_value('EMAIL_SUBJECT_CONFIRM'), u.email,
                      'confirmation_instructions', user=u,
                      confirmation_link=confirmation_link)

                #confirm_instructions_sent.send(app._get_current_object(), user=user)

                #token = confirmable.send_confirmation_instructions(u)
                #utils.send_mail("test", mail, 'send_confirmation')

                response = {
                    "status": True,
                    "message": "registered",
                    "route":"confirm",
                    #"mail":mail,
                    "roles":["view"]
                }
            else:
                response = {
                    "status": False,
                    "message": "invalid mail",
                    "route":"register"
                    #"user":mail
                }
        else:
            response = {
                "status": False,
                "message": "invalid password",
                "route":"register"
                #"user":mail
            }

    return jsonify(response )

@app.route('/login',methods=['GET'])
def login():

    n = request.args.get("next")
    #mail = json["mail"]
    #json = request.get_json()
    #n =  json["next"]
    response = {
            "status": False,
            "message": "Not authenticated",
            "route":"login",
            "next":n
        }
    return jsonify(response )
    #return render_template('index.html')
@app.route('/logout',methods=['POST'])
def logout():

    utils.logout_user()
    response = {
            "status": True,
            "message": "Not authenticated",
            "route":"home",
            "mail":"",
            "roles":[],
            "active":False
        }
    #resp.set_cookie('username', expires=0)
    #session.pop('username', None)
    return jsonify(response )
    #return render_template('index.html')
@app.route('/log',methods=['POST'])
def log():
    json = request.get_json()
    app.logger.debug(json)
    #mail = request.args.get("mail")
    mail = json["mail"]
    app.logger.debug(mail)
    #password = request.args.get("password")
    password = json["password"]
    #url = json["url"]

    u = user_datastore.get_user(mail)
    if u:

        v = utils.verify_password(password, u.password)
        if v:
            if u.confirmed_at:
                utils.login_user(u, remember=True)
                i=[]
                for v in u.roles:
                    i.append(v.name)

                response = {
                        "status": True,
                        "message": "Authenticated",
                        #"url":url,
                        "route":"home",
                        "mail":mail,
                        "roles":i,
                        "confirmed_at":u.confirmed_at,
                        "active":u.active
                    }
            else:
                response = {
                    "status": False,
                    "message": "Not logged",
                    "route":"confirm",
                    "mail":"",
                    "roles":[],
                    "active":False,
                    #"url":url
                }
        else:
            response = {
                "status": False,
                "message": "Not logged",
                "route":"login",
                "mail":"",
                "roles":[],
                "active":False,
                #"url":url
            }



    else:
        response = {
                "status": False,
                "message": "Not a user",
                "route":"register",
                "mail":"",
                "roles":[],
                "active":False,
                #"url":url
            }
    return jsonify(response )






@app.route('/instrument/getImage/<string:name>/<int:option>',methods=['POST'])
def getImage(name,option):
    #if not current_user.is_authenticated:
        #return current_app.login_manager.unauthorized()
    app.logger.debug('Recieved')
    r = recepteurs[name]()
    try:
        filtre = request.get_json()


        app.logger.debug(filtre)
    except:
        app.logger.debug('prob filtre')
        app.logger.debug(request.form)
    #filtre = request.args.get("filtre")

    resultat = r.getRequest(filtre,"getImage",option)
    #return filtre
    return jsonify(resultat)

@app.route('/instrument/getIntegre/<string:name>/<int:option>',methods=['POST'])
#@login_required
#@roles_accepted('view', 'admin')
def getIntegre(name,option):
    #if not current_user.is_authenticated:
        #return current_app.login_manager.unauthorized()

    r = recepteurs[name]()
    filtre = request.get_json()
    #filtre = request.args.get("filtre")
    resultat = r.getRequest(filtre,"getIntegre",option)

    return jsonify(resultat)



if __name__ == "__main__":
    app.debug = True
    app.run()
