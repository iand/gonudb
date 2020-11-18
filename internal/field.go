package internal

import (
	"encoding/binary"
	"math"
)

const (
	MaxUint16 = math.MaxUint16
	MaxUint24 = 0xffffff
	MaxUint32 = math.MaxUint32
	MaxUint48 = 0x0000ffffffffffff
	MaxUint64 = math.MaxUint64

	MaxInt16 = math.MaxInt16

	MaxBlockSize = MaxUint16 // maximum length of a keyfile block in bytes (must not be larger than MaxKeySize due to on-disk representation)
	MaxKeySize   = MaxUint16 // maximum length of a data record's key in bytes
	MaxDataSize  = MaxUint48 // maximum length of a data record's value in bytes
)

const (
	SizeUint16 = 2
	SizeUint24 = 3
	SizeUint32 = 4
	SizeUint48 = 6
	SizeUint64 = 8
)

func DecodeUint16(b []byte) uint16 {
	return binary.BigEndian.Uint16(b)
}

func DecodeUint32(b []byte) uint32 {
	return binary.BigEndian.Uint32(b)
}

func DecodeUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

func DecodeUint48(b []byte) uint64 {
	_ = b[5] // bounds check hint to compiler; see golang.org/issue/14808
	return uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16 | uint64(b[3])<<24 |
		uint64(b[4])<<32 | uint64(b[5])<<40
}

func DecodeUint24(b []byte) uint32 {
	_ = b[2] // bounds check hint to compiler; see golang.org/issue/14808
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16
}

func EncodeUint16(b []byte, v uint16) {
	binary.BigEndian.PutUint16(b, v)
}

func EncodeUint32(b []byte, v uint32) {
	binary.BigEndian.PutUint32(b, v)
}

func EncodeUint64(b []byte, v uint64) {
	binary.BigEndian.PutUint64(b, v)
}

func EncodeUint48(b []byte, v uint64) {
	_ = b[5] // early bounds check to guarantee safety of writes below
	b[0] = byte(v)
	b[1] = byte(v >> 8)
	b[2] = byte(v >> 16)
	b[3] = byte(v >> 24)
	b[4] = byte(v >> 32)
	b[5] = byte(v >> 40)
}

func EncodeUint24(b []byte, v uint32) {
	_ = b[3] // early bounds check to guarantee safety of writes below
	b[0] = byte(v)
	b[1] = byte(v >> 8)
	b[2] = byte(v >> 16)
	b[3] = byte(v >> 24)
}

/*

// These metafunctions describe the binary format of fields on disk

template<class T>
struct field;

template<>
struct field<std::uint8_t>
{
    static std::size_t constexpr size = 1;
    static std::uint64_t constexpr max = 0xff;
};

template<>
struct field<std::uint16_t>
{
    static std::size_t constexpr size = 2;
    static std::uint64_t constexpr max = 0xffff;
};

template<>
struct field<uint24_t>
{
    static std::size_t constexpr size = 3;
    static std::uint64_t constexpr max = 0xffffff;
};

template<>
struct field<std::uint32_t>
{
    static std::size_t constexpr size = 4;
    static std::uint64_t constexpr max = 0xffffffff;
};

template<>
struct field<uint48_t>
{
    static std::size_t constexpr size = 6;
    static std::uint64_t constexpr max = 0x0000ffffffffffff;
};

template<>
struct field<std::uint64_t>
{
    static std::size_t constexpr size = 8;
    static std::uint64_t constexpr max = 0xffffffffffffffff;
};

// read field from memory

template<class T, class U, typename std::enable_if<
    std::is_same<T, std::uint8_t>::value>::type* = nullptr>
void
readp(void const* v, U& u)
{
    auto p = reinterpret_cast<std::uint8_t const*>(v);
    u = *p;
}

template<class T, class U, typename std::enable_if<
    std::is_same<T, std::uint16_t>::value>::type* = nullptr>
void
readp(void const* v, U& u)
{
    auto p = reinterpret_cast<std::uint8_t const*>(v);
    T t;
    t =  T(*p++)<< 8;
    t =  T(*p  )      | t;
    u = t;
}

template<class T, class U, typename std::enable_if<
    std::is_same<T, uint24_t>::value>::type* = nullptr>
void
readp(void const* v, U& u)
{
    auto p = reinterpret_cast<std::uint8_t const*>(v);
    std::uint32_t t;
    t =  std::uint32_t(*p++)<<16;
    t = (std::uint32_t(*p++)<< 8) | t;
    t =  std::uint32_t(*p  )      | t;
    u = t;
}

template<class T, class U, typename std::enable_if<
    std::is_same<T, std::uint32_t>::value>::type* = nullptr>
void
readp(void const* v, U& u)
{
    auto const* p = reinterpret_cast<std::uint8_t const*>(v);
    T t;
    t =  T(*p++)<<24;
    t = (T(*p++)<<16) | t;
    t = (T(*p++)<< 8) | t;
    t =  T(*p  )      | t;
    u = t;
}

template<class T, class U, typename std::enable_if<
    std::is_same<T, uint48_t>::value>::type* = nullptr>
void
readp(void const* v, U& u)
{
    auto p = reinterpret_cast<std::uint8_t const*>(v);
    std::uint64_t t;
    t = (std::uint64_t(*p++)<<40);
    t = (std::uint64_t(*p++)<<32) | t;
    t = (std::uint64_t(*p++)<<24) | t;
    t = (std::uint64_t(*p++)<<16) | t;
    t = (std::uint64_t(*p++)<< 8) | t;
    t =  std::uint64_t(*p  )      | t;
    u = t;
}

template<class T, class U, typename std::enable_if<
    std::is_same<T, std::uint64_t>::value>::type* = nullptr>
void
readp(void const* v, U& u)
{
    auto p = reinterpret_cast<std::uint8_t const*>(v);
    T t;
    t =  T(*p++)<<56;
    t = (T(*p++)<<48) | t;
    t = (T(*p++)<<40) | t;
    t = (T(*p++)<<32) | t;
    t = (T(*p++)<<24) | t;
    t = (T(*p++)<<16) | t;
    t = (T(*p++)<< 8) | t;
    t =  T(*p  )      | t;
    u = t;
}

// read field from istream

template<class T, class U>
void
read(istream& is, U& u)
{
    readp<T>(is.data(field<T>::size), u);
}

inline
void
read_size48(istream& is, std::size_t& u)
{
    std::uint64_t v;
    read<uint48_t>(is, v);
    BOOST_ASSERT(v <= std::numeric_limits<std::uint32_t>::max());
    u = static_cast<std::uint32_t>(v);
}

// write field to ostream

template<class T, class U, typename std::enable_if<
    std::is_same<T, std::uint8_t>::value>::type* = nullptr>
void
write(ostream& os, U u)
{
    BOOST_ASSERT(u <= field<T>::max);
    std::uint8_t* p = os.data(field<T>::size);
    *p = static_cast<std::uint8_t>(u);
}

template<class T, class U, typename std::enable_if<
    std::is_same<T, std::uint16_t>::value>::type* = nullptr>
void
write(ostream& os, U u)
{
    BOOST_ASSERT(u <= field<T>::max);
    auto const t = static_cast<T>(u);
    std::uint8_t* p = os.data(field<T>::size);
    *p++ = (t>> 8)&0xff;
    *p   =  t     &0xff;
}

template<class T, class U, typename std::enable_if<
    std::is_same<T, uint24_t>::value>::type* = nullptr>
void
write(ostream& os, U u)
{
    BOOST_ASSERT(u <= field<T>::max);
    auto const t = static_cast<std::uint32_t>(u);
    std::uint8_t* p = os.data(field<T>::size);
    *p++ = (t>>16)&0xff;
    *p++ = (t>> 8)&0xff;
    *p   =  t     &0xff;
}

template<class T, class U, typename std::enable_if<
    std::is_same<T, std::uint32_t>::value>::type* = nullptr>
void
write(ostream& os, U u)
{
    BOOST_ASSERT(u <= field<T>::max);
    auto const t = static_cast<T>(u);
    std::uint8_t* p = os.data(field<T>::size);
    *p++ = (t>>24)&0xff;
    *p++ = (t>>16)&0xff;
    *p++ = (t>> 8)&0xff;
    *p   =  t     &0xff;
}

template<class T, class U, typename std::enable_if<
    std::is_same<T, uint48_t>::value>::type* = nullptr>
void
write(ostream& os, U u)
{
    BOOST_ASSERT(u <= field<T>::max);
    auto const t = static_cast<std::uint64_t>(u);
    std::uint8_t* p = os.data(field<T>::size);
    *p++ = (t>>40)&0xff;
    *p++ = (t>>32)&0xff;
    *p++ = (t>>24)&0xff;
    *p++ = (t>>16)&0xff;
    *p++ = (t>> 8)&0xff;
    *p   =  t     &0xff;
}

template<class T, class U, typename std::enable_if<
    std::is_same<T, std::uint64_t>::value>::type* = nullptr>
void
write(ostream& os, U u)
{
    auto const t = static_cast<T>(u);
    std::uint8_t* p = os.data(field<T>::size);
    *p++ = (t>>56)&0xff;
    *p++ = (t>>48)&0xff;
    *p++ = (t>>40)&0xff;
    *p++ = (t>>32)&0xff;
    *p++ = (t>>24)&0xff;
    *p++ = (t>>16)&0xff;
    *p++ = (t>> 8)&0xff;
    *p   =  t     &0xff;
}

} // detail
} // nudb

#endif

*/
