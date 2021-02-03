package com.angle;

import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;
import org.hibernate.query.Query;

import java.util.Iterator;
import java.util.List;

public class ManageEmployee {
    private static SessionFactory factory;
    public static void main(String[] args) {
        try {
         factory = new Configuration().configure().buildSessionFactory();
        }catch ( Throwable ex){
            System.err.println("Failed to create sessionFactory object." + ex);
            throw new ExceptionInInitializerError(ex);
        }

        ManageEmployee me = new ManageEmployee();
        Integer e1 = me.addEmployee("Zara", "Ali", 1000);
        Integer e2 = me.addEmployee("Daisy", "Das", 1000);
        Integer e3 = me.addEmployee("John", "Paul", 1000);
        System.out.println("--------insert three employer to table----------");
        me.listEmployees();
        System.out.println("--------update one employer to table----------");
        me.updateEmployee(e1,3000);
        me.listEmployees();
        System.out.println("--------delete one employer to table----------");
        me.deleteEmployee(e2);
        me.listEmployees();
        System.out.println("--------drop table employee----------");
//        me.dropTable();
        me.listEmployees();

        factory.close();
    }

    /**
     * 增
     * @param fame
     * @param lname
     * @param salary
     * @return
     */
    public Integer addEmployee(String fame,String lname,int salary){
        Session session = factory.openSession();
        Transaction tx = null;
        Integer employeeId = null;
        try {
            tx = session.beginTransaction();
            Employee e = new Employee(fame, lname, salary);
            employeeId = (Integer) session.save(e);
            tx.commit();
        }catch (HibernateException e){
            if(tx != null){
                tx.rollback();
            }
            e.printStackTrace();
        }finally {
            session.close();
        }
        return employeeId;
    }

    /**
     * 查
     */
    public void listEmployees(){
        Session session = factory.openSession();
        Transaction tx = null;
        try {
            tx = session.beginTransaction();
            List employees = session.createQuery("FROM Employee").list();
            for(Iterator iterator = employees.iterator();iterator.hasNext();){
                Employee employee = (Employee) iterator.next();
                System.out.print("First Name: " + employee.getFirstName());
                System.out.print("  Last Name: " + employee.getLastName());
                System.out.println("  Salary: " + employee.getSalary());
            }
            tx.commit();
        }catch (HibernateException e){
            if(tx!=null){
                tx.rollback();
            }
            e.printStackTrace();
        }finally {
            session.close();
        }
    }

    /**
     * 改
     * @param employeeId
     * @param salary
     */
    public void updateEmployee(Integer employeeId,int salary){
        Session session = factory.openSession();
        Transaction tx = null;
        try {
            tx = session.beginTransaction();
            Employee employee = session.get(Employee.class, employeeId);
            employee.setSalary(salary);
            session.update(employee);
            tx.commit();
        }catch (HibernateException e){
            if (tx != null) {
                tx.rollback();
            }
            e.printStackTrace();
        }finally {
            session.close();
        }
    }

    /**
     * 删
     * @param employeeId
     */
    public void deleteEmployee(Integer employeeId){
        Session session = factory.openSession();
        Transaction tx = null;
        try {
            tx = session.beginTransaction();
            Employee employee = session.get(Employee.class, employeeId);
            session.delete(employee);
            tx.commit();
        }catch (HibernateException e){
            if(tx != null ){
                tx.rollback();
            }
            e.printStackTrace();
        }finally {
            session.close();
        }
    }

    public void dropTable(){
        Session session = factory.openSession();
        Transaction tx = null;
        try {
            String sql = "delete from Employee where 1 = 1";
            tx = session.beginTransaction();
            Query query = session.createQuery(sql);
            query.executeUpdate();
            tx.commit();
        }catch (HibernateException e) {
            if(tx != null){
                tx.rollback();
            }
            e.printStackTrace();
        }finally {
            session.close();
        }
    }
}
