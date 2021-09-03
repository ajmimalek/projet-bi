import psycopg2
import pygrametl
from pygrametl.datasources import CSVSource
from pygrametl.tables import Dimension, FactTable
import matplotlib.pyplot as plt



pgconn = psycopg2.connect(dbname='LivraisonFournisseur',user='postgres', password='0000',port='5433')
connection = pygrametl.ConnectionWrapper(pgconn)
connection.setasdefault()
connection.execute('set search_path to "LivraisonFournisseur"')
print('connection établie')
cursor1 = pgconn.cursor()
cursor2 = pgconn.cursor()
cursor3 = pgconn.cursor()

commande_file_handle = open('Livraisons.csv', 'r', 16384)
commandeSource = CSVSource(commande_file_handle, delimiter=';')
print('lecture du fichier réussi')
# Creation of dimension and fact table abstractions for use in the ETL flow
fournisseur_dimension = Dimension(
    name='fournisseur',
    key='id',
    attributes=['adresse_fournisseur','nom_fournisseur','id_pays'])

PaysLivraison_dimension = Dimension(
    name='paysLivraison',
    key='id_pl',
    attributes=['idpays_livraison'])
 

temps_dimension = Dimension(
    name='temps',
    key='id_date',
    attributes=['annee','mois','temps_livraison','datelivraison']
    )

commande_Fact = FactTable(
    name='commande',
    keyrefs=['id','id_date','id_pl'],
    measures=['qte','prix_Commande'])

# Python function needed to split the date into its three parts
def transformerDate(row):
    """Splits a timestamp containing a date into its three parts"""

    # Splitting of the timestamp into parts
    date = row['DateLivraison']
    # Assignment of each part to the dictionary
    date_split1= date.split(' ')
    date_split=date_split1[0].split('/')
        # Récupérer chaque élément à part et le rajouter dans le dictionnaire
    row['annee'] = date_split[2]
    print('annee :' + row['annee'])
    row['mois'] = date_split[1]
    row['jours']=date_split[0]
    

for row in commandeSource:
    print(row)
    row['datelivraison']=row['DateLivraison']
    row['temps_livraison']=row['TempsLivraison']
    row['id_date']=pygrametl.getint(row['id'])
    
    
    #
    transformerDate(row)
    row['id_date']=temps_dimension.ensure(row)
    
    row['id']=pygrametl.getint(row['id'])
    row['nom_fournisseur']=row['NomFournisseur']
    row['adresse_fournisseur']=row['AddressFournisseur']
    row['id_pays']=row['idPays']
    row['id']=fournisseur_dimension.ensure(row)
##    
    row['id_pl']=pygrametl.getint(row['id'])
    row['idpays_livraison']=pygrametl.getint(row['idPaysLivraison'])
    row['id_pl']=PaysLivraison_dimension.ensure(row)
##

    row['prix_Commande']=pygrametl.getfloat(row['PrixCommande'])
    row['qte']=pygrametl.getint(row['Qte'])
##    # The row can then be inserted into the fact table
    commande_Fact.ensure(row)
    cursor2.execute("""Select Sum(qte),mois from commande c , temps t where t.id_date=c.id_date group by mois""")
    ListMois=[]
    ListQ = []
    for prix in cursor2:
        ListQ.append(prix[0])
        ListMois.append(prix[1])
    plt.xlabel("mois")
    plt.ylabel("Quantité")
    plt.title("Variation des quantités en fonction des mois")
    plt.plot(ListQ,color='r')
    plt.show()
    cursor1.execute("""Select fournisseur.id,Prix_Commande from fournisseur,commande where fournisseur.id=commande.id group by commande.Prix_Commande,fournisseur.id""")
    ListP=[]
    listN=[]
    for prix in cursor1:
        ListP.append(prix[0])
        listN.append(prix[1])
    plt.xlabel("Id fournisseur")
    plt.ylabel("Prix")
    plt.title("Le Prix proposé par chaque Fournisseur")
    plt.hist(ListP)
     
    plt.show()
    cursor3.execute("""select fournisseur.id_Pays,commande.qte from fournisseur,commande where fournisseur.id=commande.id and id_Pays=1  group by fournisseur.id_Pays,commande.qte""")
    IDPays=[]
    Qte=[]
    
    for prix in cursor3:
        IDPays.append(prix[0])
        Qte.append(prix[1])
        
    plt.pie(Qte, labels=IDPays, autopct='%1.1f%%', startangle=90, shadow=True) 
    plt.axis('equal') 
    plt.title("Les quantités commandés dans la pays 1")
    plt.show()
    
# The data warehouse connection is then ordered to commit and close
connection.commit()
connection.close()

# Finally the connection to the sales database is closed
pgconn.close()
