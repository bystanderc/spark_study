package kafka_study;

/**
 * @author bystander
 * @date 2020/4/1
 */
public class TbUser {

    private Long id;
    private String username;
    private String password;
    private String phone;
    private String created;
    private String salt;


    public TbUser() {
    }

    public TbUser(Long id, String username, String password, String phone, String created, String salt) {
        this.id = id;
        this.username = username;
        this.password = password;
        this.phone = phone;
        this.created = created;
        this.salt = salt;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public String getCreated() {
        return created;
    }

    public void setCreated(String created) {
        this.created = created;
    }

    public String getSalt() {
        return salt;
    }

    public void setSalt(String salt) {
        this.salt = salt;
    }


    @Override
    public String toString() {
        return "TbUser{" +
                "id=" + id +
                ", username='" + username + '\'' +
                ", password='" + password + '\'' +
                ", phone='" + phone + '\'' +
                ", created=" + created +
                ", salt='" + salt + '\'' +
                '}';
    }
}
