

#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/mman.h>
#include <stdlib.h> 
#include <string.h> 
#include <pthread.h> 
#include <stdatomic.h>
/* Error List*/ 
[[maybe_unused]] constexpr int FIB_ERRFAIL = -1;
/* Enum Definitions */

/* Reservation ID for the Reservation Tickets */
enum Reserv_ID : uint8_t{
	RSV_ATENDING = 0x00   , 
	RSV_WAITING = 0x01 , 
	RSV_DONE = 0x02
};

/* Enum state of FIber state */ 
enum Fib_state_ID : uint8_t {
	FIBST_RUNNING ,
	FIBST_INQUEUE, 
	FIBST_YIELDING,
	FIBST_FINSHED 
};


/* NEEDED TEMPLATE MACROS*/

#define CACHE_LINE 64ULL
#define HD_DESTRUCTIVE_INF_SZ (CACHE_LINE* 2ULL ) /* Due to agressive prefetchers on modern processor the Alignment for destructive infrence size will be set to the cache line * 2 */

/* Value of number of cores are set at compiile time to chnage this define your own */
#define NO_OF_THREADS 4ULL
#define NO_OF_HARVESTERS (NO_OF_THREADS -1ULL)
/* Struct Defintions */ 

/* thsi contains the resrvation tickets of threads that want to write to  a queue */
struct __attribute__((aligned(HD_DESTRUCTIVE_INF_SZ))) Reserv_write_tickets{
	_Atomic(uint8_t) m_mask ; /* This holds the state of the reservation tickets */	
	size_t m_position ;
};

/* This represents the reservation tickets of the Harvester hungry to steal */
struct __attribute__((aligned(HD_DESTRUCTIVE_INF_SZ))) Reserv_steal_tickets{
	_Atomic(uint8_t) m_mask ; /* This holds the state of the reservation tickets */	
	size_t m_position ;
};
struct __attribute__((aligned(CACHE_LINE)))scheduler_local{
	size_t id ;
	size_t m_write ; 
	size_t m_steal ;

};
struct __attribute__((aligned(HD_DESTRUCTIVE_INF_SZ)))Scheduler{
	alignas(HD_DESTRUCTIVE_INF_SZ) struct Reserv_steal_tickets m_steal_tickets [NO_OF_HARVESTERS] ;  
	alignas(HD_DESTRUCTIVE_INF_SZ) struct Reserv_write_tickets m_write_tickets[NO_OF_HARVESTERS];
	// Scheduler Local data 
	struct scheduler_local m_local_data;	

};
/* This saves calle saved non volatile registers for the x86 Architecture */
struct x86_registers {
	size_t rsp ; 
	size_t rbp ; 
	size_t rbx ; 
	size_t r12 ; 
	size_t r13 ; 
	size_t r14 ; 
	size_t r15 ; 
};

/* This contains the values for being entered into the function 
 * NOTE FOR SPACE CONSTRAINS FUNCTIONS WIL ONLY SUPPORT */ 
struct x86_abi_func_registers{
 	size_t rdi , rsi , rdx , rcx , r8;	
};
struct __attribute__((aligned(CACHE_LINE)))fiber_data{
	void* addr ; /* Adrress of function */
	struct x86_registers m_registers ;
	size_t m_stack_position ; 
	uint8_t m_mask ; /* This contains the state od teh function */
};

#define DEFINE_RESULT(TypeName , DataType, Alignment ) \
	struct TypeName{\
		static_assert(sizeof(DataType) <= 64 , "The Data size is greatear than 64 byte alignement " );\
		_Atomic(uint8_t) m_mask ; \
		alignas(_Alignof(DataType)) DataType m_data ;\
	}

struct mpsc_queue{

	/* Head written to by the scheduler 
	 * NOTE : DUE to the large amoutn of producer this will cause great contention on the head_ atomic value and will sat		urate the buss local copy will be kept but also only the scheduler after collation of result from the producer cores w		ill write to this varaible 
	 */ 
	alignas(HD_DESTRUCTIVE_INF_SZ) _Atomic(uint64_t) m_head_;

	/* tail is consume and gooubled up by the consumer*/
	alignas(HD_DESTRUCTIVE_INF_SZ) _Atomic(uint64_t) m_tail_;
};


/* Harvester contains data of the cores that acts as consumers */
struct Harvester{
	alignas(HD_DESTRUCTIVE_INF_SZ) unsigned char* m_stack ;
	void* m_queue;
	unsigned char* m_local_copy_buf ;
	uint32_t write_idx ; 
	uint32_t read_idx ; 
	uint32_t rem_idx ;
	uint16_t m_id ;
	void* m_result_queue;
};

/*Fiber */
struct __attribute__((aligned(HD_DESTRUCTIVE_INF_SZ))) fiber {
	struct Scheduler m_scheduler ; 
	void* m_queue[NO_OF_HARVESTERS];/* Addrresses of all the queues*/
	void* m_result_queues_ [NO_OF_HARVESTERS]; /* Addresses of Result Queues */ 
};

/* Function Declerations*/
	

/* Core Functions */ 
int fiber_switch() ; 
int fiber_trampoline();


/*Function defintions */


int main(){

	return 0;
}
