package simplejdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.sql.DataSource;

public class DAO {

	protected final DataSource myDataSource;

	/**
	 *
	 * @param dataSource la source de données à utiliser
	 */
	public DAO(DataSource dataSource) {
		this.myDataSource = dataSource;
	}

	/**
	 *
	 * @return le nombre d'enregistrements dans la table CUSTOMER
	 * @throws DAOException
	 */
	public int numberOfCustomers() throws DAOException {
		int resultat = 0;

		String requete = "SELECT COUNT(*) AS NUMBER FROM CUSTOMER";
		try (   Connection connection = myDataSource.getConnection(); 
			Statement stmt = connection.createStatement();
			ResultSet rs = stmt.executeQuery(requete)
		) {
			if (rs.next()) {
				resultat = rs.getInt("NUMBER");
			}
		} catch (SQLException ex) {
			Logger.getLogger("DAO").log(Level.SEVERE, null, ex);
			throw new DAOException(ex.getMessage());
		}

		return resultat;
	}

	/**
	 * Detruire un enregistrement dans la table CUSTOMER
	 * @param customerId la clé du client à détruire
	 * @return le nombre d'enregistrements détruits (1 ou 0 si pas trouvé)
	 * @throws DAOException
	 */
	public int deleteCustomer(int customerId) throws DAOException {

		String requete = "DELETE FROM CUSTOMER WHERE CUSTOMER_ID = ?";
		try (   Connection connection = myDataSource.getConnection();
			PreparedStatement state = connection.prepareStatement(requete);
                ) {
			state.setInt(1, customerId);
			
			return state.executeUpdate();

		}  catch (SQLException ex) {
			Logger.getLogger("DAO").log(Level.SEVERE, null, ex);
			throw new DAOException(ex.getMessage());
		}
	}
	
	/**
	 *
	 * @param customerId la clé du client à recherche
	 * @return le nombre de bons de commande pour ce client (table PURCHASE_ORDER)
	 * @throws DAOException
	 */
	public int numberOfOrdersForCustomer(int customerId) throws DAOException {
		int resultat = 0;
		String requete = "SELECT COUNT(*) AS NUMBER FROM PURCHASE_ORDER WHERE CUSTOMER_ID = ?";
		try (   Connection connection = myDataSource.getConnection();
			PreparedStatement state = connection.prepareStatement(requete);
                ) {
			state.setInt(1, customerId);
			try (ResultSet rs = state.executeQuery()) {
				rs.next();
				resultat = rs.getInt("NUMBER");
			}
		}  catch (SQLException ex) {
			Logger.getLogger("DAO").log(Level.SEVERE, null, ex);
			throw new DAOException(ex.getMessage());
		}
		return resultat;
	}

	/**
	 * Trouver un Customer à partir de sa clé
	 *
	 * @param customerID la clé du CUSTOMER à rechercher
	 * @return l'enregistrement correspondant dans la table CUSTOMER, ou null si pas trouvé
	 * @throws DAOException
	 */
	public CustomerEntity findCustomer(int customerID) throws DAOException {
		CustomerEntity resultat = null;

		String requete = "SELECT * FROM CUSTOMER WHERE CUSTOMER_ID = ?";
		try (Connection connection = myDataSource.getConnection(); 
			PreparedStatement state = connection.prepareStatement(requete);) {

			state.setInt(1, customerID);
			try (ResultSet rs = state.executeQuery()) {
				if (rs.next()) {
					String name = rs.getString("NAME");
					String address = rs.getString("ADDRESSLINE1");
					resultat = new CustomerEntity(customerID, name, address);
				} 
			}
		}  catch (SQLException ex) {
			Logger.getLogger("DAO").log(Level.SEVERE, null, ex);
			throw new DAOException(ex.getMessage());
		}

		return resultat;
	}

	/**
	 * Liste des clients localisés dans un état des USA
	 *
	 * @param state l'état à rechercher (2 caractères)
	 * @return la liste des clients habitant dans cet état
	 * @throws DAOException
	 */
	public List<CustomerEntity> customersInState(String state) throws DAOException {
		List<CustomerEntity> resultat = new LinkedList<>();

		String sql = "SELECT * FROM CUSTOMER WHERE STATE = ?";
		try (Connection connection = myDataSource.getConnection();
			PreparedStatement statem = connection.prepareStatement(sql)) {

			statem.setString(1, state);

			try (ResultSet rs = statem.executeQuery()) {
				while (rs.next()) {
					int id = rs.getInt("CUSTOMER_ID");
					String name = rs.getString("NAME");
					String address = rs.getString("ADDRESSLINE1");
					CustomerEntity c = new CustomerEntity(id, name, address);
					resultat.add(c);
				}
			}
		}  catch (SQLException ex) {
			Logger.getLogger("DAO").log(Level.SEVERE, null, ex);
			throw new DAOException(ex.getMessage());
		}

		return resultat;

	}

}
