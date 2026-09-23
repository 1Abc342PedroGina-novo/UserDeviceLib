#ifndef CLASS_IPC_HPP
#define CLASS_IPC_HPP

// Se compilado dentro do Kernel Linux, usamos os tipos nativos do módulo. 
// Caso contrário (User Space), usamos os tipos padrão do C++ standard.
#ifdef __KERNEL__
    #include <linux/types.h>
    #include <linux/string.h>
    #include <linux/slab.h>
    
    // Mapeamento explícito de tipos primitivos para compatibilidade perfeita
    typedef uint32_t size_t;
#else
    #include <cstdint>
    #include <cstddef>
    #include <cstring>
#endif

namespace Reflector {

// 4.1 Tipos de Universos (Portas)
enum class PortType : uint32_t {
    KERNEL = 0,
    USER = 1,
    SYSTEM = 2
};

// 5.4 Níveis de Prioridade
enum class MsgPriority : uint32_t {
    NORMAL = 0,
    URGENT = 1,
    POWER = 2
};

// Layout binário idêntico e alinhado (packed) para tráfego via DMA/Barramento
struct [[gnu::packed]] PortAddress {
    PortType type;
    char namespace_str[32];
    char name[32];
    uint32_t id;
};

// Cabeçalho unificado de mensagem IPC
struct [[gnu::packed]] MessageHeader {
    PortAddress src;
    PortAddress dst;
    uint32_t opcode;
    uint32_t sequence;
    MsgPriority priority;
    uint32_t payload_len;
};

// Classe abstrata/canal que opera em ambos os universos de forma transparente
class IpcChannel {
private:
    int channel_fd; // Usado apenas no User Space (-1 no Kernel)

public:
    IpcChannel();
    ~IpcChannel();

    // Inicializa o canal (User Space abre /dev/reflector, Kernel associa estruturas internas)
    bool connect(const char* device_path = "/dev/reflector");
    void disconnect();

    // Interface de Envio (Ponteiros brutos para compatibilidade mútua kernel/user)
    int send_message(const PortAddress& src, const PortAddress& dst, uint32_t opcode, 
                     uint32_t sequence, MsgPriority priority, const uint8_t* payload, size_t payload_len);

    // Interface de Recebimento
    int receive_message(MessageHeader& out_header, uint8_t* out_payload_buf, size_t max_buf_len);
};

} // namespace Reflector

#endif // CLASS_IPC_HPP
