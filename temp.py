import psycopg2
import pygrametl
from pygrametl.datasources import CSVSource
from pygrametl.tables import Dimension, FactTable


pgconn = psycopg2.connect(dbname='LivraisonFournisseur', user='postgres', password='0000', port='5433')
connection = pygrametl.ConnectionWrapper(pgconn)
connection.setasdefault()
connection.execute('set search_path to "LivraisonFournisseur"')
print('connection établie')

livraison_file_handle = open('Livraisons.csv', 'r', 16384)
LivraisonSource = CSVSource(livraison_file_handle, delimiter=';')
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
    
    # Splitting of the timestamp into parts
    date = row['DateLivraison']

    # Assignment of each part to the dictionary
    date_split = date.split('/')
    
    # Récupérer chaque élément à part et le rajouter dans le dictionnaire
    row['anneeLivraison'] = date_split[2][:4]
    print('annee :' + row['annee'])
    row['moisLivraison'] = date_split[1]
    print('mois :' + row['mois'])
    


for row in LivraisonSource:
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
    row['qte']=pygrametl.getfloat(row['Qte'])
##    # The row can then be inserted into the fact table
    commande_Fact.ensure(row)

    

# The data warehouse connection is then ordered to commit and close
connection.commit()
connection.close()

# Finally the connection to the sales database is closed
pgconn.close()
