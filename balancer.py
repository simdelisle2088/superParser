import asyncio
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import logging
import os
import smtplib
from typing import List
import aiohttp
from dotenv import load_dotenv


#Environement Variable
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USERNAME = os.getenv("SMTP_USERNAME")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
ALERT_EMAILS = os.getenv("ALERT_EMAILS", "").split(",")

# Server URLs
FETCH_URL = os.getenv("FETCH_URL")
FETCH2_URL = os.getenv("FETCH2_URL")

server_map = {True: 'St-Hubert', False: 'St-Jean'}

def send_alert_email(subject: str, message: str, recipients: List[str]):
    try:
        msg = MIMEMultipart()
        msg['From'] = SMTP_USERNAME
        msg['To'] = ", ".join(recipients)
        msg['Subject'] = f"[ALERTE SERVEUR] {subject}"

        msg.attach(MIMEText(f"""
        <html>
            <body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
                <div style="max-width: 600px; margin: 0 auto; padding: 20px;">
                    <h2 style="color: #2c3e50; border-bottom: 2px solid #eee; padding-bottom: 10px;">
                        Alerte État du Serveur
                    </h2>
                    <div style="margin: 20px 0;">
                        <p><strong>Statut:</strong> {subject}</p>
                        <p><strong>Date et heure:</strong> {datetime.now().strftime('%d-%m-%Y %H:%M:%S UTC')}</p>
                        <p><strong>Détails:</strong></p>
                        <p style="padding: 10px; background-color: #f8f9fa; border-left: 4px solid #2c3e50;">
                            {message}
                        </p>
                    </div>
                    <div style="margin-top: 30px; padding-top: 20px; border-top: 1px solid #eee; font-size: 12px; color: #666;">
                        <p>Ceci est un message automatique du système de surveillance des serveurs.</p>
                        <p>Merci de ne pas répondre à cet email.</p>
                    </div>
                </div>
            </body>
        </html>
        """, 'html'))

        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USERNAME, SMTP_PASSWORD)
            server.send_message(msg)
            
        logging.info(f"Email sent to {recipients}")
    except Exception as e:
        logging.error(f"Failed to send Email {e}")

async def check_server_status():
    fetch_down = False
    timeout = aiohttp.ClientTimeout(total=30)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        while True:
            try:
                logging.info("Verification du serveur de St-Hubert...")
                headers = {'X-Health-Check': 'true'}
                async with session.get(f"{FETCH_URL}/status", headers=headers) as fetch_response:
                    status_code = fetch_response.status
                    
                    if status_code != 200:
                        if status_code == 503:  # Service Unavailable - possibly due to DB load
                            logging.info("Server is busy with database operations, waiting longer...")
                            await asyncio.sleep(30)  # Wait longer before next check
                            continue
                
                        logging.warning(f"Server returned status code: {status_code}")
                        if not fetch_down:
                            await server_unavailable(session)
                            fetch_down = True
                    else:
                        try:
                            response_data = await fetch_response.json()
                            
                            if fetch_down or response_data.get('status') == 'stopped':
                                logging.info("Le serveur fetch est en ligne. Retour au serveur principal...")
                                
                                # Use await for these requests
                                async with session.get(f"{FETCH2_URL}/stop") as _:
                                    pass
                                    
                                async with session.get(f"{FETCH_URL}/start") as _:
                                    pass

                                send_alert_email(
                                    "Serveur Fetch Rétabli",
                                    "Le serveur fetch est de nouveau en ligne. Retour au serveur principal effectué.",
                                    ALERT_EMAILS
                                )
                                fetch_down = False
                            else:
                                logging.info(f"{server_map[fetch_down]}, {response_data}")
                                
                        except ValueError as json_error:
                            logging.error(f"Failed to parse JSON response: {json_error}")
                            if not fetch_down:
                                await server_unavailable(session)
                                fetch_down = True
                            
            except aiohttp.ClientError as e:
                error_msg = f"Erreur de connexion lors de la verification du serveur ({FETCH_URL}): {str(e)}"
                logging.error(error_msg)
                
                if not fetch_down:
                    try:
                        async with session.get(f"{FETCH2_URL}/start") as backup_response:
                            if backup_response.status != 200:
                                raise Exception(f"Backup server returned status {backup_response.status}")
                                
                        send_alert_email(
                            "Erreur de Connexion au Serveur Fetch",
                            f"{error_msg}\nLe serveur fetch de St-Jean a été démarré en secours.",
                            ALERT_EMAILS
                        )
                        fetch_down = True
                    except Exception as backup_error:
                        logging.error(f"Erreur lors du démarrage du serveur de secours ({FETCH2_URL}): {backup_error}")
                        
            except Exception as e:
                if not fetch_down:
                    error_msg = "Impossible d'atteindre le serveur de St-Hubert" if not str(e) else f"Erreur inattendue: {e}"
                    logging.error(error_msg)
                    await server_unavailable(session)
                    fetch_down = True

            await asyncio.sleep(30)

async def server_unavailable(session):
    logging.warning(f"The fetch server is down. Starting the St-Jean server...")
    # Start fetch2 server
    send_alert_email(
        "Serveur Fetch Hors Service",
        f"Le serveur fetch rencontre des problèmes. Le serveur fetch de St-Jean a été démarré en tant que solution de secours.",
        ALERT_EMAILS
    )
    async with session.get(f"{FETCH_URL}/start") as _:
        
        pass